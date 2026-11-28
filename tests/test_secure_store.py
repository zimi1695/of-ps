import ctypes
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from utils import secure_store as ss


class SecureStoreDaclTests(unittest.TestCase):
    def dacl(self, path):
        _, kernel32, advapi32 = ss._windows_api()
        descriptor = ctypes.c_void_p()
        advapi32.GetNamedSecurityInfoW.argtypes = [
            ctypes.c_wchar_p, ctypes.c_int, ctypes.c_uint,
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
            ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        # SE_FILE_OBJECT, OWNER|GROUP|DACL_SECURITY_INFORMATION. The returned
        # descriptor is absolute, unlike GetFileSecurityW's relative form.
        status = advapi32.GetNamedSecurityInfoW(
            str(path), 1, 0x7, None, None, None, None, ctypes.byref(descriptor))
        if status != 0:
            raise ss.SecureStoreError("GetNamedSecurityInfoW failed: %d" % status)
        text = ctypes.c_wchar_p()
        try:
            if not advapi32.ConvertSecurityDescriptorToStringSecurityDescriptorW(
                    descriptor, 1, 0x7, ctypes.byref(text), None):
                raise ss.SecureStoreError("cannot read DACL")
            return text.value
        finally:
            if text:
                kernel32.LocalFree(text)
            kernel32.LocalFree(descriptor)


@unittest.skipUnless(os.name == "nt", "DPAPI requires Windows")
class SecureStoreTests(SecureStoreDaclTests, unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "nested" / "secret.bin"
        self.store = ss.SecureStore(self.path)

    def test_round_trip_arbitrary_bytes_and_no_plaintext_on_disk(self):
        for payload in (b"", b"x", bytes(range(256)), os.urandom(65536)):
            with self.subTest(length=len(payload)):
                self.store.write_bytes(payload)
                self.assertEqual(self.store.read_bytes(), payload)
                if len(payload) >= 16:
                    self.assertNotIn(payload, self.path.read_bytes())
        self.assertTrue(self.path.read_bytes().startswith(ss._MAGIC))

    def test_dpapi_rejects_wrong_entropy(self):
        self.store.write_bytes(b"secret")
        with self.assertRaises(ss.CorruptSecretError):
            ss.unprotect_bytes(self.path.read_bytes(), b"wrong-context")
        with self.assertRaises(TypeError):
            self.store.write_bytes("not bytes")

    def test_truncation_bitflip_and_header_corruption_fail_closed(self):
        self.store.write_bytes(b"a secret that must never leak")
        original = self.path.read_bytes()
        corruptions = [b"", b"plaintext", original[:-1], original + b"x",
                       ss._MAGIC + b"\x00\x00\x00\x00",
                       ss._MAGIC + b"\xff\xff\xff\xff" + original[ss._HEADER_SIZE:]]
        flipped = bytearray(original)
        flipped[-1] ^= 0x80
        corruptions.append(bytes(flipped))
        for corrupted in corruptions:
            with self.subTest(length=len(corrupted)):
                self.path.write_bytes(corrupted)
                with self.assertRaises(ss.CorruptSecretError):
                    self.store.read_bytes()
        self.store.write_bytes(b"still usable after explicit rewrite")
        self.assertEqual(self.store.read_bytes(), b"still usable after explicit rewrite")

    def test_atomic_replace_failure_preserves_original_and_cleans_temp(self):
        self.store.write_bytes(b"old")
        original = self.path.read_bytes()
        with patch.object(ss.os, "replace", side_effect=OSError("simulated failure")):
            with self.assertRaises(OSError):
                self.store.write_bytes(b"new")
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(list(self.path.parent.glob("*.tmp")), [])
        self.assertEqual(list(self.path.parent.glob(".*.tmp")), [])

    def test_encryption_failure_does_not_touch_original(self):
        self.store.write_bytes(b"old")
        original = self.path.read_bytes()
        with patch.object(ss, "protect_bytes", side_effect=ss.SecureStoreError("failed")):
            with self.assertRaises(ss.SecureStoreError):
                self.store.write_bytes(b"new")
        self.assertEqual(self.path.read_bytes(), original)

    def test_rotation_changes_ciphertext_and_old_entropy_stops_working(self):
        self.store.write_bytes(b"rotate me")
        old_ciphertext = self.path.read_bytes()
        old_entropy = self.store.entropy
        self.store.rotate_entropy(b"new-context")
        self.assertEqual(self.store.read_bytes(), b"rotate me")
        self.assertNotEqual(self.path.read_bytes(), old_ciphertext)
        with self.assertRaises(ss.CorruptSecretError):
            ss.SecureStore(self.path, old_entropy).read_bytes()

    def test_failed_rotation_preserves_file_and_entropy(self):
        self.store.write_bytes(b"old")
        original = self.path.read_bytes()
        entropy = self.store.entropy
        with patch.object(ss, "protect_bytes", side_effect=ss.SecureStoreError("failed")):
            with self.assertRaises(ss.SecureStoreError):
                self.store.rotate_entropy(b"new")
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(self.store.entropy, entropy)
        self.assertEqual(self.store.read_bytes(), b"old")

    def test_atomic_writes_restrict_dacl_to_current_user_and_system(self):
        self.store.write_bytes(b"acl test")
        dacl = self.dacl(self.path)
        self.assertIn("D:P", dacl)  # Protected, not inherited from a shared directory.
        self.assertNotIn("Everyone", dacl)
        self.assertNotIn("BU)", dacl)
        self.assertIn("SY)", dacl)
        self.assertEqual(ss.atomic_write(self.path, self.path.read_bytes()), None)

    def test_concurrent_readers_never_observe_partial_writes(self):
        payloads = [bytes([index]) * 32768 for index in range(8)]
        self.store.write_bytes(payloads[0])
        failures = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                try:
                    value = self.store.read_bytes()
                    if value not in payloads:
                        failures.append("partial or invalid value")
                        return
                except Exception as exc:  # Any exception here is a failed atomic write.
                    failures.append(repr(exc))
                    return

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            for index in range(24):
                self.store.write_bytes(payloads[index % len(payloads)])
        finally:
            stop.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])

    def test_missing_file_is_not_silently_recreated(self):
        with self.assertRaises(FileNotFoundError):
            self.store.read_bytes()


class UnsupportedPlatformTests(unittest.TestCase):
    def test_non_windows_never_falls_back(self):
        path = Path(tempfile.gettempdir()) / "unused"
        with patch.object(ss.os, "name", "posix"):
            with self.assertRaises(ss.UnsupportedPlatformError):
                ss.protect_bytes(b"secret")
            with self.assertRaises(ss.UnsupportedPlatformError):
                ss.unprotect_bytes(ss._MAGIC + b"\x00\x00\x00\x01x")
            with self.assertRaises(ss.UnsupportedPlatformError):
                ss.atomic_write(path, b"secret")


if __name__ == "__main__":
    unittest.main()

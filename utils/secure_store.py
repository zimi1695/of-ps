"""Windows DPAPI secret storage with atomic, permission-restricted writes.

CryptProtectData/CryptUnprotectData protect arbitrary bytes.  Entropy is an
application-level context (e.g. a purpose/domain string), not a password or a
substitute for a machine/user backup policy.  DPAPI master-key loss, profile
loss or a different Windows user/machine makes ciphertext unrecoverable.
Non-Windows platforms fail explicitly; plaintext fallback is forbidden.
"""

import ctypes
import os
import tempfile
import threading


class SecureStoreError(OSError):
    """Storage or DPAPI operation failed."""


class UnsupportedPlatformError(SecureStoreError):
    """The requested secure storage operation is unsupported."""


class CorruptSecretError(SecureStoreError):
    """Ciphertext is malformed, tampered with or cannot be decrypted."""


_MAGIC = b"OFPSDP1"
_HEADER_SIZE = len(_MAGIC) + 4


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_uint32), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


def _blob(data):
    # Keep the backing buffer alive for the duration of the native call.
    buffer = ctypes.create_string_buffer(data, max(1, len(data)))
    pointer = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))
    return _DataBlob(len(data), pointer), buffer


def _windows_api():
    if os.name != "nt":
        raise UnsupportedPlatformError("DPAPI secure storage requires Windows")
    try:
        crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    except (AttributeError, OSError) as exc:
        raise UnsupportedPlatformError("Windows DPAPI APIs are unavailable") from exc
    return crypt32, kernel32, advapi32


def protect_bytes(data, entropy=b"ofps-secure-store-v1"):
    """Encrypt bytes for the current Windows user; return a framed blob."""
    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    if not isinstance(entropy, bytes):
        raise TypeError("entropy must be bytes")
    crypt32, kernel32, _ = _windows_api()
    source, source_buffer = _blob(data)
    salt, salt_buffer = _blob(entropy)
    destination = _DataBlob()
    flags = 1  # CRYPTPROTECT_UI_FORBIDDEN
    try:
        if not crypt32.CryptProtectData(ctypes.byref(source), None, ctypes.byref(salt),
                                        None, None, flags, ctypes.byref(destination)):
            raise SecureStoreError("DPAPI encryption failed: Windows error %d" %
                                   ctypes.get_last_error())
        ciphertext = ctypes.string_at(destination.pbData, destination.cbData)
    finally:
        source_buffer, salt_buffer = source_buffer, salt_buffer
        if destination.pbData:
            kernel32.LocalFree(destination.pbData)
    if len(ciphertext) > 0xFFFFFFFF:
        raise SecureStoreError("DPAPI ciphertext is too large")
    return _MAGIC + len(ciphertext).to_bytes(4, "big") + ciphertext


def unprotect_bytes(blob, entropy=b"ofps-secure-store-v1"):
    """Authenticate/decrypt a protect_bytes result; fail closed on corruption."""
    if not isinstance(blob, bytes) or not isinstance(entropy, bytes):
        raise TypeError("blob and entropy must be bytes")
    crypt32, kernel32, _ = _windows_api()
    if not blob.startswith(_MAGIC) or len(blob) < _HEADER_SIZE:
        raise CorruptSecretError("invalid secure-store header")
    length = int.from_bytes(blob[len(_MAGIC):_HEADER_SIZE], "big")
    ciphertext = blob[_HEADER_SIZE:]
    if length == 0 or length != len(ciphertext):
        raise CorruptSecretError("invalid secure-store length")
    source, source_buffer = _blob(ciphertext)
    salt, salt_buffer = _blob(entropy)
    destination = _DataBlob()
    try:
        if not crypt32.CryptUnprotectData(ctypes.byref(source), None, ctypes.byref(salt),
                                          None, None, 1, ctypes.byref(destination)):
            raise CorruptSecretError("DPAPI decryption/authentication failed: Windows error %d" %
                                     ctypes.get_last_error())
        return ctypes.string_at(destination.pbData, destination.cbData)
    finally:
        source_buffer, salt_buffer = source_buffer, salt_buffer
        if destination.pbData:
            kernel32.LocalFree(destination.pbData)


def _current_user_sddl(advapi32):
    """Build a protected DACL allowing only this logon's user and SYSTEM."""
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    # Pointer-sized handles must not be truncated by ctypes' default c_int.
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    advapi32.OpenProcessToken.argtypes = [ctypes.c_void_p, ctypes.c_ulong,
                                          ctypes.POINTER(ctypes.c_void_p)]
    advapi32.GetTokenInformation.argtypes = [ctypes.c_void_p, ctypes.c_int,
                                             ctypes.c_void_p, ctypes.c_ulong,
                                             ctypes.POINTER(ctypes.c_ulong)]
    advapi32.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p,
                                                ctypes.POINTER(ctypes.c_wchar_p)]
    token = ctypes.c_void_p()
    if not advapi32.OpenProcessToken(kernel32.GetCurrentProcess(), 0x0008, ctypes.byref(token)):
        raise SecureStoreError("OpenProcessToken failed: Windows error %d" %
                               ctypes.get_last_error())
    try:
        # TOKEN_USER == 1; first obtain the required buffer size.
        needed = ctypes.c_uint32()
        advapi32.GetTokenInformation(token, 1, None, 0, ctypes.byref(needed))
        buffer = ctypes.create_string_buffer(needed.value)
        if not advapi32.GetTokenInformation(token, 1, buffer, needed, ctypes.byref(needed)):
            raise SecureStoreError("GetTokenInformation failed: Windows error %d" %
                                   ctypes.get_last_error())
        sid = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_void_p))[0]
        sid_string = ctypes.c_wchar_p()
        if not advapi32.ConvertSidToStringSidW(sid, ctypes.byref(sid_string)):
            raise SecureStoreError("ConvertSidToStringSidW failed: Windows error %d" %
                                   ctypes.get_last_error())
        try:
            return "D:P(A;;FA;;;SY)(A;;FA;;;%s)" % sid_string.value
        finally:
            kernel32.LocalFree(sid_string)
    finally:
        kernel32.CloseHandle(token)


def _restrict_file_permissions(path):
    if os.name != "nt":
        raise UnsupportedPlatformError("permission-restricted storage requires Windows")
    _, kernel32, advapi32 = _windows_api()
    sddl = _current_user_sddl(advapi32)
    descriptor = ctypes.c_void_p()
    if not advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl, 1, ctypes.byref(descriptor), None):
        raise SecureStoreError("invalid DACL: Windows error %d" % ctypes.get_last_error())
    try:
        # FILE_ALL_ACCESS, DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION
        if not advapi32.SetFileSecurityW(str(path), 0x40000004, descriptor):
            raise SecureStoreError("SetFileSecurityW failed: Windows error %d" %
                                   ctypes.get_last_error())
    finally:
        kernel32.LocalFree(descriptor)
    try:
        os.chmod(path, 0o600)
    except OSError as exc:
        raise SecureStoreError("cannot restrict file mode") from exc


def atomic_write(path, data):
    """Write ciphertext bytes: same-directory temp, fsync, ACL, atomic replace."""
    path = os.path.abspath(os.fspath(path))
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + os.path.basename(path) + ".",
                                     suffix=".tmp", dir=parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        _restrict_file_permissions(temporary)
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


class SecureStore:
    """Atomic encrypted file store. Instance operations are serialized."""

    def __init__(self, path, entropy=b"ofps-secure-store-v1"):
        self.path = os.path.abspath(os.fspath(path))
        self.entropy = entropy
        self._lock = threading.RLock()

    def write_bytes(self, data):
        encrypted = protect_bytes(data, self.entropy)
        with self._lock:
            atomic_write(self.path, encrypted)

    def read_bytes(self):
        with self._lock:
            try:
                with open(self.path, "rb") as stream:
                    encrypted = stream.read()
            except FileNotFoundError:
                raise
            return unprotect_bytes(encrypted, self.entropy)

    def rotate_entropy(self, new_entropy):
        """Re-encrypt with new entropy; failures leave the original file intact.

        The caller must durably update the entropy/context configuration before
        or together with this operation. DPAPI's own master-key rotation remains
        an operating-system responsibility, not an application API.
        """
        if not isinstance(new_entropy, bytes):
            raise TypeError("entropy must be bytes")
        with self._lock:
            plaintext = self.read_bytes()
            encrypted = protect_bytes(plaintext, new_entropy)
            atomic_write(self.path, encrypted)
            self.entropy = new_entropy

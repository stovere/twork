import base64
import os
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


class AESCrypto:
    def __init__(self, aes_key: bytes):
        if len(aes_key) != 32:
            raise ValueError("AES key must be 32 bytes for AES-256-CBC.")
        self.aes_key = aes_key

    def aes_encode(self, data) -> str:
        # 确保输入是字符串
        if not isinstance(data, str):
            data = str(data)

        iv = os.urandom(16)  # 生成 16 字节 IV
        cipher = AES.new(self.aes_key, AES.MODE_CBC, iv)
        encrypted = cipher.encrypt(pad(data.encode('utf-8'), AES.block_size))

        # 拼接 IV 和密文，并 Base64 编码
        encrypted_with_iv = base64.b64encode(iv + encrypted).decode('utf-8')

        # 转换为 URL-safe Base64
        url_safe = encrypted_with_iv.replace('+', '-').replace('/', '_').rstrip('=')
        return url_safe

    def aes_decode(self, url_safe_encrypted: str) -> str:
        # 恢复 Base64 填充
        padding_needed = (4 - len(url_safe_encrypted) % 4) % 4
        base64_encoded = url_safe_encrypted + ('=' * padding_needed)
        base64_encoded = base64_encoded.replace('-', '+').replace('_', '/')

        decoded_data = base64.b64decode(base64_encoded)
        iv = decoded_data[:16]
        encrypted_data = decoded_data[16:]

        cipher = AES.new(self.aes_key, AES.MODE_CBC, iv)
        decrypted = unpad(cipher.decrypt(encrypted_data), AES.block_size)

        return decrypted.decode('utf-8')

import os
from utils.aes_crypto import AESCrypto
from news_config import AES_KEY


if __name__ == "__main__":
    key = "fjd#jdsfdlsm3234fsdf"  # 与 PHP 示例相同
    aes = AESCrypto(AES_KEY)

    data = "Hello, World!"
    encrypted_py = aes.encrypt(data)
    print("Python 端加密：", encrypted_py)
    print("Python 端解密：", aes.decrypt(encrypted_py))

    encrypted_py2 = "zTUE1d0fgnG9aVxsICYE4Vp22AVs0bvE23J5H2qxngs"  # 由php端加密的字符串
    print("Python 端解密：", aes.decrypt(encrypted_py2))



    # 如果你在 PHP 端执行:
    #   $enc = new tgbot_common();
    #   echo $enc->aes_encode("Hello, World!");
    # 得到的字符串（假设为 $php_enc），你可以在 Python 这样解密：
    #   print(aes.decrypt($php_enc))
    #
    # 同理，Python 端 encrypt() 的输出，也可以用 PHP 的 aes_decode() 解密。

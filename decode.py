from utils.aes_crypto import AESCrypto


AES_KEY = "fjd#jdsfdlsm3234fsdf"
crypto = AESCrypto(AES_KEY)
# encdoe = crypto.aes_encode("s;DuEe6;7gLNgI")
encdoe = "wkYzOqSHRpNElLaeg_e5R1hCR3dwY3hKY1loQ0lONzFRUUJPQWc9PQ"
# encdoe = "GmarknvSQUUj-wUWRg-wQ1hnOG84VDJXQzR5U3lKVzJUNXFHSkE9PQ"
print(f"{encdoe}")
print(crypto.aes_decode(encdoe))
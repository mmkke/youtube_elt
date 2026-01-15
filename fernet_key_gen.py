from cryptography.fernet import Fernet

# 1. Generate the key
fernet_key = Fernet.generate_key()

# 2. Print the key (optional)
print(f"Generated Fernet Key: {fernet_key.decode()}")

# 3. Use the key (example)
f = Fernet(fernet_key)
token = f.encrypt(b"my secret data")
print(f"Encrypted Token: {token}")
print(f"Decrypted Token: {f.decrypt(token)}")
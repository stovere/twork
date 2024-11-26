import asyncio
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, AuthKeyDuplicatedError, RPCError
import os
from telethon.sessions import StringSession

# Check if running in a local development environment
if not os.getenv('GITHUB_ACTIONS'):
    from dotenv import load_dotenv
    load_dotenv()

# Get values from environment variables
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
phone_number = os.getenv('PHONE_NUMBER')
pw2fa = os.getenv('PW2FA')
session_password = os.getenv('SESSION_PASSWORD')
session_name = api_id + 'session_name'  # Ensure it matches the uploaded session file name

session_file = session_name + '.session'

stringsession_file = session_name + '.stringsession.enc'



session_string = os.getenv('SESSION_STRING')

session_name = StringSession(session_string)

# Create the client
client = TelegramClient(session_name, api_id, api_hash)

async def login():
    
    try:
        # Send verification code and start the login process
        print("Sending verification code to the specified phone number...", flush=True)
        await client.send_code_request(phone_number)

        # User inputs the received verification code
        code = input('Please enter the code you received(a): ')  
       
        # Use phone number and verification code to log in
        await client.sign_in(phone=phone_number, password=pw2fa, code=code)
        await save_session_to_file(client, stringsession_file)
       



    except SessionPasswordNeededError:
        # Handle two-factor authentication
        print("Two-factor authentication password is required", flush=True)
        await client.sign_in(password=pw2fa)

        await save_session_to_file(client, stringsession_file)

    except RPCError as e:
        # Capture RPC error and display detailed error message
        print(f"Failed to send verification request, error: {e}", flush=True)  
    except Exception as e:
        print(f"An unexpected error occurred during login: {e}")        
    finally:
        return True       


async def save_session_to_file(client, file_path):
    try:
        string_session = client.session.save()
        with open(file_path, 'w') as file:
            file.write(string_session)
        print(f"String session saved to {file_path}")
    except Exception as e:
        print(f"Failed to save session file: {e}")

async def encrypt_session_file(input_file, output_file, password):
    try:
        # 构建 OpenSSL 加密命令
        openssl_command = [
            'openssl', 'aes-256-cbc', '-pbkdf2', '-salt',
            '-in', input_file, '-out', output_file,
            '-pass', f'pass:{password}'
        ]

        # 使用 asyncio.create_subprocess_exec 异步执行命令
        process = await asyncio.create_subprocess_exec(
            *openssl_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        # 等待进程完成并获取输出
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            print(f"Encryption successful. Encrypted file saved as {output_file}")
        else:
            print(f"Encryption failed: {stderr.decode()}")

    except Exception as e:
        print(f"Encryption failed with exception: {str(e)}")

async def main():
    try:
        # Attempt to start the client
        print("Attempting to start the client...", flush=True)
        await client.connect()
    except AuthKeyDuplicatedError:
        # Capture AuthKeyDuplicatedError, delete the old session file and re-login
        print("Detected duplicate authorization key, deleting old session file and retrying login...", flush=True)
        await client.disconnect()  # Disconnect the client
        client.session.close()  # Ensure the session is closed
        try:
            if os.path.exists(session_file):
                os.remove(session_file)
                print(f"已删除会话文件: {session_file}", flush=True)
            if os.path.exists(session_file + ".enc"):
                os.remove(session_file + ".enc")
                print(f"已删除加密的会话文件: {session_file}.enc", flush=True)
        except FileNotFoundError:
            print("删除会话文件时未找到文件。", flush=True)
        await asyncio.sleep(1)  # 间隔1秒

        await client.connect()
        # Reconnect after deleting the old session
        #
        # 检查客户端是否已连接
        if not client.is_connected():
            print("客户端连接失败，请检查网络连接或 API 密钥配置。", flush=True)
            return

    # Check if the user is already authorized
    if not await client.is_user_authorized():
        print("User is not authorized, starting the login process...", flush=True)
        await login()
    else:
        print("User is already authorized, no need to log in again", flush=True)

    print(f"\n\nopenssl aes-256-cbc -pbkdf2 -salt -in {session_file} -out {session_file}.enc -pass pass:{session_password}\n\n")
    await encrypt_session_file(session_file, session_file+".enc", session_password)


# Explicitly control client startup process instead of using `with client:`
if __name__ == '__main__':
    try:
        # client.start(phone=phone_number)
        client.loop.run_until_complete(main())
    except AuthKeyDuplicatedError:
        print("Handling AuthKeyDuplicatedError, deleting old session file...", flush=True)
        client.disconnect()  # Disconnect before deleting session file
        client.session.close()  # Ensure session file is released
        if os.path.exists(session_file):
            os.remove(session_file)
        print("Session file deleted, please rerun the program to verify...", flush=True)
    except Exception as e:
        print(f"An exception occurred: {e}", flush=True)

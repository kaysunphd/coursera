import hvac
import sys

def init_server():
    """Initialize a hvac client at http://localhost:8200 and
    prints out whether the client is authenticated.
    """
    client = hvac.Client(url='http://localhost:8200')
    print(f" Is client authenticated: {client.is_authenticated()}")
    return client

def write_secret(secret_path, key, value):
    """
    Writes key=value pair to secret/secret_path for client and 
    prints out information about the secret written, such as
    its creation time.
    """
    create_response = client.secrets.kv.v2.create_or_update_secret(path=secret_path, secret={key: value})
    print(create_response)

def read_secret(secret_path):
    """
    Reads secrets from secret/secret_path for client and 
    prints out information about the secret read.
    """
    read_response = client.secrets.kv.v2.read_secret_version(path=secret_path)
    print(read_response)

def delete_secret(secret_path):
    """
    Delete secrets from secret/secret_path for client.
    """
    client.secrets.kv.v2.delete_latest_version_of_secret(path=secret_path)

if __name__ == "__main__":
    client = init_server()
    if len(sys.argv) < 3:
        print("Insufficient arguments inputted. Please include the method name AND secret path.")
        sys.exit(1)
    method_name = sys.argv[1]
    path = sys.argv[2]

    if str(method_name) == "write_secret":
        if len(sys.argv) < 5:
            print("Insufficient arguments inputted. Please include the secret key AND value.")
            sys.exit(1)    
        key = sys.argv[3]
        value = sys.argv[4]
        write_secret(path, key, value)
    
    elif str(method_name) == "read_secret":
        if len(sys.argv) > 3:
            print("Too many arguments. Please only include the secret path.")
            sys.exit(1)  
        read_secret(path)
    
    elif str(method_name) == "delete_secret":
        if len(sys.argv) > 3:
            print("Too many arguments. Please only include the secret path.")
            sys.exit(1)  
        delete_secret(path)

    else:
        print("Please input one of the valid methods: write_secret OR read_secret OR delete_secret")
        sys.exit(1)
     

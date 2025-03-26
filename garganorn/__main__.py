"""Main entry point for Garganorn."""
from lexrpc import Server
from garganorn import load_lexicons

def main():
    """Start the Garganorn server."""
    print("Garganorn server starting...")
    
    lexicons = load_lexicons()
    if not lexicons:
        print("No lexicons found. Server will have no endpoints.")
    
    server = Server(lexicons=lexicons)
    # Start the server
    server.run(host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()

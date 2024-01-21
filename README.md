# IceDrive Blob service
## Usage

    bash
    pip install -e.

For server aplication:  

    icedrive-blob --Ice.Config=config/blob.config

For the client application (change the code in app.py following the instructions in the comments to test the functionality of the client application):  
WARNING: This is not implemented for task 2. My apologies

    icedrive-client --Ice.Config=config/datatransfer.config "{proxy sent by the server aplication standart output}"

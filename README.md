WARNING: The application doesn't work for the second task yet. There is the whole funcionality of the application for the task 1 in past commits (last one before January 2024).
# IceDrive Blob service
## Usage

    bash
    pip install -e.

For server aplication (for the task 1):  

    icedrive-blob --Ice.Config=config/blob.config

For the client application (for the task 1. Change the code in app.py following the instructions in the comments to test the functionality of the client application):  
WARNING: This is not implemented for task 2. My apologies.

    icedrive-client --Ice.Config=config/datatransfer.config "{proxy sent by the server aplication standart output}"

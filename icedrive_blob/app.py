"""Blob service application."""

import logging
import sys
from typing import List

import Ice
import IceDrive
from icedrive_blob.blob import BlobService, DataTransfer


class BlobApp(Ice.Application):
    """Implementation of the Ice.Application for the Blob service."""

    def run(self, args: List[str]) -> int:
        """Execute the code for the BlobApp class."""
        adapter = self.communicator().createObjectAdapter("BlobAdapter")
        adapter.activate()

        servant = BlobService()
        servant_proxy = adapter.addWithUUID(servant)

        logging.info("Proxy: %s", servant_proxy)

        self.shutdownOnInterrupt()
        self.communicator().waitForShutdown()

        return 0

class ClientApp(Ice.Application):
    
    def run(self, args: List[str]) -> int:
        #if len(args) != 2:
            #print("Usage: python client.py <proxy>")
            #return 2
        print(args)
        proxy = self.communicator().stringToProxy(args[1])
        blob_prx = IceDrive.BlobServicePrx.checkedCast(proxy)
        if not blob_prx:
            print("Invalid proxy")
            return 2
        
        # blob_prx.upload()
        adapter = self.communicator().createObjectAdapter("DataTransferAdapter")
        adapter.activate()

        datatransferUploaded = DataTransfer("../ficherosCliente/prueba.txt")
        datatransferUploaded_proxy = adapter.addWithUUID(datatransferUploaded)
        blob_id = blob_prx.upload(datatransferUploaded_proxy)
        print ("Blob id: ", blob_id)

        # blob_prx.link()
        blob_prx.link(blob_id)

        # blob_prx.unlink()
        blob_prx.unlink(blob_id)

        # blob_prx.download()
        datatransferDownloaded_proxy = blob_prx.download(blob_id)

        full_data = b''
        data = datatransferDownloaded_proxy.read(1024)
        while(data):
            full_data += data
            data = datatransferDownloaded_proxy.read(1024)
        print(full_data)
        
    


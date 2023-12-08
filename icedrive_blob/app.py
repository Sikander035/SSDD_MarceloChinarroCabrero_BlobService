"""Blob service application."""

import logging
import sys
from typing import List

import Ice
import IceDrive

from icedrive_blob.blob import BlobService


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
        if len(args) != 2:
            print("Usage: python client.py <proxy>")
            return 2
        
        proxy = self.communicator().stringToProxy(args[1])
        blob_prx = IceDrive.BlobServicePrx.checkedCast(proxy)
        if not blob_prx:
            print("Invalid proxy")
            return 2

        ##blob_prx.link("hola")    

        return 0
    


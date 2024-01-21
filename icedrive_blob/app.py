"""Blob service application."""

import threading
import time
import logging
import sys
from typing import List

import Ice
import IceDrive
import IceStorm
from icedrive_blob.blob import BlobService
from icedrive_blob.discovery import Discovery


class BlobApp(Ice.Application):
    """Implementation of the Ice.Application for the Blob service."""

    """Announce the service every 5 seconds."""
    def announce(publisher: IceDrive.DiscoveryPrx, discovery_proxy: IceDrive.BlobServicePrx) -> None:
        while True:
            # Announce the Blob service through the IceDrive.DiscoveryPrx every 5 seconds
            publisher.announceBlobService(discovery_proxy)
            time.sleep(5)

    def run(self, args: List[str]) -> int:
        """Execute the code for the BlobApp class."""
        # Create and activate the object adapter for the Blob service
        adapter = self.communicator().createObjectAdapter("BlobAdapter")
        adapter.activate()

        # Retrieve the properties from the configuration file
        properties = self.communicator().getProperties()

        # Retrieve the IceStorm.TopicManagerPrx
        topic_manager = IceStorm.TopicManagerPrx.checkedCast(
            self.communicator().propertyToProxy("IceStorm.TopicManager.Proxy")
        )

        # Instantiate the BlobService and Discovery objects
        servantDiscovery = Discovery()

        blob_topic_name = properties.getProperty("BlobQueryTopic")
        try:
            # Try to retrieve the IceStorm topic or handle the NoSuchTopic exception
            channel = topic_manager.retrieve(blob_topic_name)
        except IceStorm.NoSuchTopic:
            print("Topic does not exist")

        servantBlob = BlobService(servantDiscovery, channel.getPublisher())

        # Add the BlobService and Discovery objects to the object adapter with unique UUIDs
        blob_proxy = adapter.addWithUUID(servantBlob)
        discovery_proxy = adapter.addWithUUID(servantDiscovery)

        # Log the Blob proxy information
        logging.info("Blob Proxy: %s", blob_proxy)

        # Setup IceStorm
        discovery_topic_name = properties.getProperty("DiscoveryTopic")

        try:
            # Try to retrieve the IceStorm topic or handle the NoSuchTopic exception
            channel = topic_manager.retrieve(discovery_topic_name)
        except IceStorm.NoSuchTopic:
            print("Topic does not exist")

        # Subscribe to the IceStorm topic with the Discovery proxy
        publisher = IceDrive.DiscoveryPrx.uncheckedCast(channel.getPublisher())
        channel.subscribeAndGetPublisher({}, discovery_proxy)

        # Start a new thread for announcing the Blob service
        threading.Thread(target=BlobApp.announce, args=(publisher, IceDrive.BlobServicePrx.uncheckedCast(blob_proxy))).start()

        # Set up shutdown behavior on interrupt and wait for the communicator to finish
        self.shutdownOnInterrupt()
        self.communicator().waitForShutdown()

        return 0
        

class ClientApp(Ice.Application):
    
    def run(self, args: List[str]) -> int:
        
        proxy = self.communicator().stringToProxy(args[1])
        blob_prx = IceDrive.BlobServicePrx.checkedCast(proxy)
        if not blob_prx:
            print("Invalid proxy")
            return 2
        
        ### blob_prx.upload() --- Uncomentt the following lines to test the upload method
        #adapter = self.communicator().createObjectAdapter("DataTransferAdapter")
        #adapter.activate()
        #datatransferUploaded = DataTransfer("ficherosCliente/prueba.txt")
        #datatransferUploaded_proxy = adapter.addWithUUID(datatransferUploaded)
        #blob_id = blob_prx.upload(IceDrive.DataTransferPrx.uncheckedCast(datatransferUploaded_proxy))
        #print ("Blob id: ", blob_id)

        ### blob_prx.link() --- Uncomentt the following lines to test the link method
        #blob_prx.link(blob_id)

        ### blob_prx.unlink() --- Uncomentt the following lines to test the unlink method
        #blob_prx.unlink(blob_id)

        ### blob_prx.download() --- Uncomentt the following lines to test the download method
        #datatransferDownloaded_proxy = blob_prx.download(blob_id)
        #full_data = b''
        #data = datatransferDownloaded_proxy.read(1024)
        #while(data):
            #full_data += data
            #data = datatransferDownloaded_proxy.read(1024)
        #print(full_data)
        
    


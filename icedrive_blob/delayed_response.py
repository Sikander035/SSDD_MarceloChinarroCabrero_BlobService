"""Servant implementation for the delayed response mechanism."""

import Ice
import IceDrive
import threading

class BlobQueryResponse(IceDrive.BlobQueryResponse):
    """Query response receiver."""

    def __init__(self, future: Ice.Future):
        self.future_callback = future

    def downloadBlob(self, blob: IceDrive.DataTransferPrx, current: Ice.Current = None) -> None:
        """Receive a `DataTransfer` when other service instance knows the `blob_id`."""
        self.future_callback.set_result(blob)
        current.adapter.remove(current.id)

    def blobLinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was linked."""
        self.future_callback.set_result(None)
        current.adapter.remove(current.id)

    def blobUnlinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was unlinked."""
        self.future_callback.set_result(None)
        current.adapter.remove(current.id)

    def blobIdExists(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was unlinked."""
        self.future_callback.set_result(None)
        current.adapter.remove(current.id)
        

class BlobQuery(IceDrive.BlobQuery):
    """Query receiver."""

    def __init__(self, blob_service):
        self.blob_service = blob_service

    def downloadBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query for downloading an archive based on `blob_id`."""
        try:
            blob = self.blob_service.return_data_transfer(blob_id)
            print("Download query received")
            response.downloadBlob(blob)

        except NotImplementedError:
            pass

    def linkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to create a link for `blob_id` archive if it exists."""
        try:
            self.blob_service.link(blob_id)
            print("Link query received")
            response.blobLinked()

        except NotImplementedError:
            pass

    def unlinkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to destroy a link for `blob_id` archive if it exists."""
        try:
            self.blob_service.unlink(blob_id)
            print("Unlink query received")
            response.blobUnlinked()

        except NotImplementedError:
            pass
    
    def blobIdExists(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to check if a blob_id exists."""
        try:
            if self.blob_service.blobExists(blob_id):
                print("BlobIdExists query received")
                response.blobIdExists(blob_id)
            else:
                raise Exception
                
        except NotImplementedError:
            pass
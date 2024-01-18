"""Servant implementation for the delayed response mechanism."""

import Ice
import IceDrive
import threading

class BlobQueryResponse(IceDrive.BlobQueryResponse):
    """Query response receiver."""

    def __init__(self):
        self.future_callback = Ice.Future()

    def downloadBlob(self, blob: IceDrive.DataTransferPrx, current: Ice.Current = None) -> None:
        """Receive a `DataTransfer` when other service instance knows the `blob_id`."""
        self.future_callback.set_result(blob)
        current.adapter.remove(current.id)

    def blobLinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was linked."""
        self.future_callback.set_result(True)
        current.adapter.remove(current.id)

    def blobUnlinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was unlinked."""
        self.future_callback.set_result(False)
        current.adapter.remove(current.id)

class BlobQuery(IceDrive.BlobQuery):
    """Query receiver."""

    def __init__(self, blob_service):
        self.blob_service = blob_service

    def downloadBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query for downloading an archive based on `blob_id`."""
        try:
            blob = self.blob_service.download(blob_id)
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
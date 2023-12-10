import unittest
from unittest.mock import MagicMock, patch
from blob import BlobService, IceDrive

class TestBlobService(unittest.TestCase):
    def setUp(self):
        self.blob_service = BlobService()

    def test_link_existing_blob(self):
        blob_id = "123456"
        self.blob_service.blobs = [{'blobId': blob_id, 'numLinks': 0, 'name': 'file.txt'}]
        self.blob_service.link(blob_id)
        self.assertEqual(self.blob_service.blobs[0]['numLinks'], 1)

    def test_link_unknown_blob(self):
        blob_id = "123456"
        with self.assertRaises(IceDrive.UnknownBlob):
            self.blob_service.link(blob_id)

    def test_unlink_existing_blob(self):
        blob_id = "123456"
        self.blob_service.blobs = [{'blobId': blob_id, 'numLinks': 1, 'name': 'file.txt'}]
        self.blob_service.unlink(blob_id)
        self.assertEqual(len(self.blob_service.blobs), 0)

    def test_unlink_unknown_blob(self):
        blob_id = "123456"
        with self.assertRaises(IceDrive.UnknownBlob):
            self.blob_service.unlink(blob_id)

    def test_upload_new_blob(self):
        datatransfer = MagicMock()
        datatransfer.read.return_value = b'Test data'
        blob_id = self.blob_service.upload(datatransfer)
        self.assertEqual(len(self.blob_service.blobs), 1)
        self.assertEqual(self.blob_service.blobs[0]['blobId'], blob_id)

    @patch('blob.open', create=True)
    def test_upload_existing_blob(self, mock_open):
        datatransfer = MagicMock()
        datatransfer.read.return_value = b'Test data'
        blob_id = "123456"
        self.blob_service.blobs = [{'blobId': blob_id, 'numLinks': 0, 'name': 'file.txt'}]
        self.blob_service.upload(datatransfer)
        self.assertEqual(len(self.blob_service.blobs), 1)
        self.assertEqual(self.blob_service.blobs[0]['blobId'], blob_id)

    def test_download_existing_blob(self):
        blob_id = "123456"
        self.blob_service.blobs = [{'blobId': blob_id, 'numLinks': 0, 'name': 'file.txt'}]
        data_transfer = self.blob_service.download(blob_id)
        self.assertIsInstance(data_transfer, IceDrive.DataTransferPrx)

    def test_download_unknown_blob(self):
        blob_id = "123456"
        with self.assertRaises(ValueError):
            self.blob_service.download(blob_id)

if __name__ == '__main__':
    unittest.main()
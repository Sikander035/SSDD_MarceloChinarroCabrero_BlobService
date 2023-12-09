"""Module for servants implementations."""
import json
import hashlib
import Ice

import IceDrive

class DataTransfer(IceDrive.DataTransfer):
    """Implementation of an IceDrive.DataTransfer interface."""

    def __init__(self, file_path : str):
        self.file = open(file_path, 'rb')

    def read(self, size: int, current: Ice.Current = None) -> bytes:
        """Returns a list of bytes from the opened file."""
        if self.file:
            data = self.file.read(size)
            # Si no hay más datos para leer, se retorna un bloque de bytes de menor tamaño
            return data
        else:
            raise RuntimeError("No file is open for reading")

    def close(self, current: Ice.Current = None) -> None:
        """Close the currently opened file."""
        if self.file:
            self.file.close()
            self.file = None
            current.adapter.remove(current.id)
        else:
            raise RuntimeError("No file is open to close")

class BlobService(IceDrive.BlobService):
    """Implementation of an IceDrive.BlobService interface."""

    def __init__(self):
        self.blobs_file = 'persistencia.json'  # Archivo para almacenar los blobs
        try:
            with open(self.blobs_file, 'r') as f:
                self.blobs = json.load(f)['blobs']
        except FileNotFoundError:
            print("File not found")
            return 2

    def save_blobs(self):
        """Save"""
        with open(self.blobs_file, 'w') as f:
            json.dump({'blobs': self.blobs}, f)

    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""
        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                blob['numLinks'] += 1
                break
        else:
            raise RuntimeError("No blob with the given blob_id")
        self.save_blobs()

    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                blob['numLinks'] -= 1
                if blob['numLinks'] == 0:
                    self.blobs.remove(blob)
                break
        self.save_blobs()

    def readwholefile(self, datatransfer: IceDrive.DataTransferPrx, current: Ice.Current = None) -> bytes:
        full_data = b''
        data = datatransfer.read(1024)
        while(data):
            full_data += data
            data = datatransfer.read(1024)
        return full_data


    def upload(self, datatransfer: IceDrive.DataTransferPrx, current: Ice.Current = None) -> str:
        """Register a DataTransfer object to upload a file to the service."""

        data = self.readwholefile(datatransfer)

        blob_id = hashlib.sha256(data).hexdigest()  # Generar un blobId único basado en el hash SHA256 de los datos

        # Verificar si el blob ya existe
        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                break
        # Si el blob no existe, añadirlo a la lista
        else:
            self.blobs.append({'blobId': blob_id, 'numLinks': 0, 'name': datatransfer.file_name})

        return blob_id

    def download(self, blob_id: str, current: Ice.Current = None) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer object to enable the client to download the given blob_id."""
        if blob_id in self.blobs:
            data = self.blobs[blob_id]
            data_transfer = IceDrive.DataTransferI(data)  # Crear una instancia de DataTransfer
            proxy = current.adapter.addWithUUID(data_transfer)  # Añadir el objeto al adaptador de objetos
            return IceDrive.DataTransferPrx.uncheckedCast(proxy)  # Devolver el proxy
        else:
            raise RuntimeError("No blob with the given blob_id")
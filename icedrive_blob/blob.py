"""Module for servants implementations."""
import uuid
import Ice

import IceDrive

class DataTransfer(IceDrive.DataTransfer):
    """Implementation of an IceDrive.DataTransfer interface."""

    def __init__(self, file_name : str):
        self.file = open("../ficheros/" + file_name, 'rb')

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
        self.blobs = {}  # Diccionario para almacenar los blobs
        self.links = {}  # Diccionario para almacenar el número de enlaces a cada blob

    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""
        if blob_id in self.links:
            self.links[blob_id] += 1
        else:
            self.links[blob_id] = 1

    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        if blob_id in self.links:
            self.links[blob_id] -= 1
            if self.links[blob_id] == 0:
                del self.links[blob_id]
                del self.blobs[blob_id]

    def upload(self, datatransfer: IceDrive.DataTransferPrx, current: Ice.Current = None) -> str:
        """Register a DataTransfer object to upload a file to the service."""
        blob_id = str(uuid.uuid4())  # Generar un blobId único
        self.blobs[blob_id] = datatransfer.read()
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

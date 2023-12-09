"""Module for servants implementations."""
import json
import hashlib
import random
import os
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
            raise IceDrive.FailedToReadData("No file is open for reading")

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
            raise IceDrive.UnknownBlob("No blob with the given blob_id")
        self.save_blobs()

    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                blob['numLinks'] -= 1
                if blob['numLinks'] == 0:
                    self.blobs.remove(blob)
                break
        else:
            raise IceDrive.UnknownBlob("No blob with the given blob_id")
        self.save_blobs()

    def read_the_whole_file(self, datatransfer: IceDrive.DataTransferPrx, current: Ice.Current = None) -> bytes:
        full_data = b''
        data = datatransfer.read(1024)
        while(data):
            full_data += data
            data = datatransfer.read(1024)
        return full_data


    def upload(self, datatransfer: IceDrive.DataTransferPrx, current: Ice.Current = None) -> str:
        """Register a DataTransfer object to upload a file to the service."""

        data = self.read_the_whole_file(datatransfer)

        blob_id = hashlib.sha256(data).hexdigest()  # Generar un blobId único basado en el hash SHA256 de los datos

        # Verificar si el blob ya existe
        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                break
        # Si el blob no existe, añadirlo a la lista
        else:
            nombre_aleatorio = str(random.randint(0, 99999999))

            # Comprobar que el nombre del archivo no se repita
            existing_names = set(blob['name'] for blob in self.blobs)
            while nombre_aleatorio + '.txt' in existing_names:
                nombre_aleatorio = str(random.randint(0, 99999999))

            # Añadir el blob a la lista    
            self.blobs.append({'blobId': blob_id, 'numLinks': 0, 'name': nombre_aleatorio + '.txt'})
            self.save_blobs()

            # Guardar el archivo generado en la carpeta "../ficheros"
            try:
                with open("../ficheros/" + nombre_aleatorio + '.txt', 'wb') as f:
                    f.write(data)
            except IOError as e:
                print(f"Error at saving the file: {e}")
        
        return blob_id

    def download(self, blob_id: str, current: Ice.Current = None) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer object to enable the client to download the given blob_id."""

        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                name = blob['name']
                break
        else:
            raise ValueError(f"No blob with the id {blob_id}")

        if os.path.exists("../ficheros/" + name):
            data_transfer = DataTransfer("../ficheros/" + name)  # Crear una instancia de DataTransfer
            proxy = current.adapter.addWithUUID(data_transfer)  # Añadir el objeto al adaptador de objetos
            return IceDrive.DataTransferPrx.uncheckedCast(proxy)  # Devolver el proxy
        else:
            raise IceDrive.UnknownBlob("No file with the given blob_id")

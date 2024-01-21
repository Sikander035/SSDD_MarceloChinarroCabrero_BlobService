"""Module for servants implementations."""
import json
import hashlib
import random
import os
import threading
import Ice
import IceStorm
from delayed_response import BlobQuery, BlobQueryResponse 

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
        
class NoResponseException(Exception):
    """Excepción personalizada para indicar que no se ha recibido ninguna respuesta."""
    def __init__(self, identity):
        self.identity = identity
        super().__init__(f"No se ha recibido ninguna respuesta para la identidad: {identity}")

class BlobService(IceDrive.BlobService):
    """Implementation of an IceDrive.BlobService interface."""

    def __init__(self, discoveryServant, queryPublisher):
        self.blobs_file = 'persistencia.json'  # Archivo para almacenar los blobs
        try:
            with open(self.blobs_file, 'r') as f:
                self.blobs = json.load(f)['blobs']
        except FileNotFoundError:
            print("File not found")
            return 2
        
        self.discovery = discoveryServant
        self.queryPublisher = queryPublisher # Publisher para publicar las queries
        self.expected_responses = {} # Diccionario para almacenar las respuestas esperadas

    def save_blobs(self):
        """Save"""
        with open(self.blobs_file, 'w') as f:
            json.dump({'blobs': self.blobs}, f)

    def remove_object_if_exists(self, adapter: Ice.ObjectAdapter, identity: Ice.Identity) -> None:
        # No one has answered 
        if adapter.find(identity) is not None:
            adapter.remove(identity)
            # definir otra excepción genérica
            self.expected_responses[identity].set_exception(NoResponseException(identity))

        del self.expected_responses[identity]

    def prepare_amd_response_callback(self, current: Ice.Current) -> IceDrive.BlobQueryResponsePrx:
        future = Ice.Future()
        response = BlobQueryResponse(future)
        prx = current.adapter.addWithUUID(response)
        query_response_prx = IceDrive.BlobQueryResponsePrx.uncheckedCast(prx)

        identity = query_response_prx.ice_getIdentity()
        self.expected_responses[identity] = future
        threading.Timer(5.0, self.remove_object_if_exists, (current.adapter, identity)).start()
        return query_response_prx

    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""
        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                blob['numLinks'] += 1
                break
        else:
            # raise IceDrive.UnknownBlob("No blob with the given blob_id")
            query_response_prx = self.prepare_amd_response_callback(current)
            self.queryPublisher.linkBlob(blob_id, query_response_prx)
            self.expected_responses[query_response_prx.ice_getIdentity()].result()
        
        self.save_blobs()

    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                blob['numLinks'] -= 1
                if blob['numLinks'] <= 0:
                    file_path = "ficheros/" + blob['name']
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    self.blobs.remove(blob)
                break
        else:
            # raise IceDrive.UnknownBlob("No blob with the given blob_id")
            query_response_prx = self.prepare_amd_response_callback(current)
            self.queryPublisher.UnlinkBlob(blob_id, query_response_prx)
            self.expected_responses[query_response_prx.ice_getIdentity()].result()
        
        self.save_blobs()

    def read_the_whole_file(self, datatransfer: IceDrive.DataTransferPrx, current: Ice.Current = None) -> bytes:
        full_data = b''
        data = datatransfer.read(1024)
        while(data):
            full_data += data
            data = datatransfer.read(1024)
        return full_data

    def verify_user(self, user: IceDrive.UserPrx) -> None:
        # Obtener auth_prx a partir de discovery
        auth_prx = self.discovery.popValid(self.discovery.authentication_services)

        # Verificar el usuario
        if not auth_prx.verifyUser(user):
            raise IceDrive.TemporaryUnavailable("User could not be verified")
        
    def get_name(self, blob_id: str) -> str:
        name = ""
        for blob in self.blobs:
            if blob['blobId'] == blob_id:
                name = blob['name']
                break
        return name
    
    def blobExists(self, blob_id: str) -> bool:
        name = self.get_name(blob_id)
        if os.path.exists("ficheros/" + name):
            return True
        else:
            return False


    def upload(self, user: IceDrive.UserPrx, datatransfer: IceDrive.DataTransferPrx, current: Ice.Current = None) -> str:
        """Register a DataTransfer object to upload a file to the service."""

        # Verificar el usuario
        self.verify_user(user)

        data = self.read_the_whole_file(datatransfer)

        blob_id = hashlib.sha256(data).hexdigest()  # Generar un blobId único basado en el hash SHA256 de los datos

        # Verificar si el blob no existe en este servidor
        if not self.blobExists(blob_id):
            
                # Verificar si el blob existe en otro servidor (resolución diferida)
                query_response_prx = self.prepare_amd_response_callback(current)
                self.queryPublisher.blobIdExists(blob_id, query_response_prx)

                try:
                    self.expected_responses[query_response_prx.ice_getIdentity()].result()
                    return blob_id
                
                except :  

                    # Si el blob no existe en ningún servidor
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
                        with open("ficheros/" + nombre_aleatorio + '.txt', 'wb') as f:
                            f.write(data)
                    except IOError as e:
                        print(f"Error at saving the file: {e}")
        
        return blob_id
    
    def return_data_transfer(self, blob_id: str, current: Ice.Current = None) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer object to enable the client to download the given blob_id."""
        name = self.get_name(blob_id)
        data_transfer = DataTransfer("ficheros/" + name)  # Crear una instancia de DataTransfer
        proxy = current.adapter.addWithUUID(data_transfer)  # Añadir el objeto al adaptador de objetos
        return IceDrive.DataTransferPrx.uncheckedCast(proxy)  # Devolver el proxy   

    def download(self, user: IceDrive.UserPrx, blob_id: str, current: Ice.Current = None) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer object to enable the client to download the given blob_id."""

        # Verificar el usuario
        self.verify_user(user)

        name = self.get_name(blob_id)

        if not os.path.exists("ficheros/" + name):
            # raise IceDrive.UnknownBlob("No blob with the given blob_id")
            query_response_prx = self.prepare_amd_response_callback(current)
            self.queryPublisher.downloadBlob(blob_id, query_response_prx)
            return self.expected_responses[query_response_prx.ice_getIdentity()]
        else:
            return self.return_data_transfer(blob_id, current)
            
            
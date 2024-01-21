"""Servant implementations for service discovery."""

import Ice

import IceDrive


class Discovery(IceDrive.Discovery):
    """Servants class for service discovery."""

    def __init__(self):
        self.authentication_services = []
        self.directory_services = []
        self.blob_services = []

    def announceAuthentication(self, prx: IceDrive.AuthenticationPrx, current: Ice.Current = None) -> None:
        """Receive an Authentication service announcement."""
        self.authentication_services.append(prx)
        print("Authentication:", prx)

    def announceDirectoryService(self, prx: IceDrive.DirectoryServicePrx, current: Ice.Current = None) -> None:
        """Receive a Directory service announcement."""
        self.directory_services.append(prx)
        print("Directory:", prx)

    def announceBlobService(self, prx: IceDrive.BlobServicePrx, current: Ice.Current = None) -> None:
        """Receive a Blob service announcement."""
        self.blob_services.append(prx)
        print("Blob:", prx)

    def popValid(list_services):
        while list_services:
            try: 
                service = list_services.pop()
                service.ice_ping()
                return service
            except Ice.NoEndpointException:
                continue
            except KeyError:
                return None
        

        
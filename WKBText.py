from  struct import  unpack



if __name__=="__main__":
    test=b'\x82\t\x00\x00\x01\x15\xa0\x99\x99\x99\x1f\x7f\x1dA\x84\xc0\xca\xe1\xa3pFA0\x89A\xe0U\x7f\x1dA\xb8\xf3\xfd\x04\xa4pFA\x00\xdc$\x06\x81E\\@\x000\x08\xac\x1cB\\@'
    print(bytearray(test).hex())
    format=unpack('<bbbb',test[:4])
    type =unpack(">BBBB",test[4:8])
    print(format,type)



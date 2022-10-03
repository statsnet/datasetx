class Test:
    name: str = None
    def __getitem__(self, __name: str):
        c= Test()
        c.name = __name
        return c


t = Test()

print(t["test"])

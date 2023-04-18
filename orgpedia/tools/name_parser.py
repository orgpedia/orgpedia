SALUTATIONS = ["Mr.", "Mrs.", "Ms.", "Dr.", "Prof."]


class NameParser:
    def parse(self, name):
        # remove any parentheses from the name
        name = name.replace("(", "").replace(")", "")
        # split the name by spaces
        parts = name.split()
        # initialize the attributes of the Name object
        salutation = ""
        first_name = ""
        last_name = ""
        initials = []

        # loop through the parts of the name
        for part in parts:
            # if the part is in the salutations list, assign it to salutation
            if part in SALUTATIONS:
                salutation = part
            # else if the part is one letter followed by a dot, add it to initials
            elif len(part) == 2 and part.endswith("."):
                initials += [part]
            # else if the part is not the last one, assign it to first_name
            elif part != parts[-1]:
                first_name = part
            # else assign it to last_name
            else:
                last_name = part

        # return a Name object with the parsed attributes
        return Name(salutation, first_name, last_name, initials)


class Name:
    def __init__(self, salutation, first_name, last_name, initials):
        self.salutation = salutation
        self.first_name = first_name
        self.last_name = last_name
        self.initials = initials

    def __str__(self):
        l = [self.salutation, self.first_name, ' '.join(self.initials), self.last_name]
        l = [n for n in l if n]
        return " ".join(l)

    @property
    def name(self):
        l = [self.first_name, ' '.join(self.initials), self.last_name]
        l = [n for n in l if n]
        return " ".join(l)

    @property
    def full_name(self):
        return str(self)

from more_itertools import first

SALUTATIONS = ["Mrs", "Mr", "Ms", "Dr", "Prof", "Miss"]


class NameParser:
    def __init__(self, extra_salutations=[]):
        all_salutations = sorted(SALUTATIONS + extra_salutations, key=len, reverse=True)

        self.expanded_salutations = []
        for s in all_salutations:
            s = s.lower()
            p = f"{s} .-{s} -{s}. -({s}) -({s}.) -({s}.)-{s}."
            self.expanded_salutations.extend(p.split("-"))

    def parse(self, name):
        # remove salutation first
        name_lower = name.lower()

        saluts = self.expanded_salutations
        found_salut = first([s for s in saluts if name_lower.startswith(s)], "")
        salutation = name[: len(found_salut)]
        salutation = salutation.strip()

        remaining_name = name[len(found_salut) :]

        # split the name by spaces
        parts = remaining_name.split()
        first_name = ""
        last_name = ""
        initials = []

        # loop through the parts of the name
        for part in parts:
            # if the part is one letter followed by a dot, add it to initials
            if len(part) == 2 and part.endswith("."):
                initials += [part]
            # else if the part is not the last one, assign it to first_name
            elif part != parts[-1]:
                first_name = part if not first_name else f'{first_name} {part}'
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

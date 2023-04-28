import pytest

from orgpedia.tools.name_parser import NameParser


def test_parse_name_with_salutation():
    parser = NameParser()
    name = parser.parse("Mr. John Smith")
    assert name.salutation == "Mr."
    assert name.first_name == "John"
    assert name.last_name == "Smith"
    assert name.initials == []


def test_parse_name_with_initials():
    parser = NameParser()
    name = parser.parse("John D. Smith")
    assert name.salutation == ""
    assert name.first_name == "John"
    assert name.last_name == "Smith"
    assert name.initials == ["D."]


def test_parse_name_with_extra_salutations():
    parser = NameParser(extra_salutations=["Sir"])
    name = parser.parse("Sir John Smith")
    assert name.salutation == "Sir"
    assert name.first_name == "John"
    assert name.last_name == "Smith"
    assert name.initials == []


def test_parse_name_with_parentheses():
    parser = NameParser()
    name = parser.parse("(Dr.) John Smith")
    assert name.salutation == "(Dr.)"
    assert name.first_name == "John"
    assert name.last_name == "Smith"
    assert name.initials == []


def test_parse_name_with_nospace():
    parser = NameParser()
    name = parser.parse("Dr.John Smith")
    assert name.salutation == "Dr."
    assert name.first_name == "John"
    assert name.last_name == "Smith"
    assert name.initials == []

import docint

import orgpedia  # noqa F401


def test_end2end():
    viz = docint.load("tests/end2end.yml")
    input_path = "order.pdf"

    doc = viz(input_path)
    print(doc.pdf_name)
    assert 1 == 1

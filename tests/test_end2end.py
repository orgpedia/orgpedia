import docint
import orgpedia


def test_end2end():
    viz = docint.load("end2end.yml")
    input_path = "order.pdf"
    
    doc = viz(input_path)
    assert 1 == 1

    

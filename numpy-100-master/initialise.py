"""Initialize and interact with NumPy exercises.

Provides functions to display questions, hints, and answers from the
NumPy 100 exercises collection.
"""

import numpy as np

import generators as ge


def question(n):
    """Display the nth question from the exercises.
    
    Args:
        n: The question number (1-100).
    """
    print(f'{n}. ' + ge.QHA[f'q{n}'])


def hint(n):
    """Display the hint for the nth question.
    
    Args:
        n: The question number (1-100).
    """
    print(ge.QHA[f'h{n}'])


def answer(n):
    """Display the answer to the nth question.
    
    Args:
        n: The question number (1-100).
    """
    print(ge.QHA[f'a{n}'])


def pick():
    """Display a random question from the exercises."""
    n = np.random.randint(1, 100)
    question(n)

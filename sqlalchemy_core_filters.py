from sqlalchemy import MetaData, Table, Column, String, Integer, create_engine
from sqlalchemy import Text, DateTime, Boolean, select, insert, update, delete, or_, and_

connection_string ="sqlite:///Northwind_small.sqlite"
engine = create_engine(connection_string, echo=False)

metadata = MetaData()
employees = Table('Employee', metadata,
                  Column('Id', Integer(), primary_key=True),
                  Column('LastName', String(8000)),
                  Column('FirstName', String(8000)),
                  Column('BirthDate', String(8000))
                  )

def print_result(stmt):
    with engine.connect() as con:
        rs = con.execute(stmt)
        for row in rs:
            print(row)

def select_all():
    stmt = employees.select()
    print_result(stmt)

def select_equals():
    stmt = employees.select().where(employees.c.Id == 1)
    print_result(stmt)

def select_not_equals():
    stmt = employees.select().where(employees.c.Id !=1)
    print_result(stmt)

def select_greater_than():
    stmt = employees.select().where(employees.c.Id >5)
    print_result(stmt)

def select_less_than():
    stmt = employees.select().where(employees.c.Id <3)
    print_result(stmt)


def select_like():
    stmt = employees.select().where(employees.c.LastName.like("Pe%k"))
    print_result(stmt)

def select_not_like():
    stmt = employees.select().where(employees.c.LastName.not_like("Pe%k"))
    print_result(stmt)


def select_contains():
    stmt = employees.select().where(employees.c.LastName.contains("u"))
    print_result(stmt)

def select_startswith():
    stmt = employees.select().where(employees.c.LastName.startswith("D"))
    print_result(stmt)


def select_endswith():
    stmt = employees.select().where(employees.c.LastName.endswith("n"))
    print_result(stmt)

def select_in_():
    stmt = employees.select().where(employees.c.Id.in_([1,2,3]))
    print_result(stmt)

def select_not_in():
    stmt = employees.select().where(employees.c.Id.not_in([1,2,3]))
    print_result(stmt)

def select_with_python_and():
    stmt = employees.select().where(
        (employees.c.Id ==7) & (employees.c.LastName =="King")
        )
    print_result(stmt)

def select_and_():
    stmt = employees.select().where(
        and_((employees.c.Id == 7), (employees.c.LastName == "King"))
        )
    print_result(stmt)

def select_multiple_where():
    stmt = employees.select().where(employees.c.Id == 7).where(employees.c.LastName == "King")

    print_result(stmt)


def select_with_python_or():
    stmt = employees.select().where(
        (employees.c.Id ==2) | (employees.c.Id ==3)
        )
    print_result(stmt)

def select_or_():
    stmt = employees.select().where(
        or_((employees.c.Id == 2), (employees.c.Id == 3))
        )
    print_result(stmt)

def select_partial():
    stmt= select(employees.c.LastName, employees.c.FirstName).where(employees.c.Id == 4)
    print_result(stmt)


if __name__ == '__main__':
    print("----- select_all() ----")
    select_all()

    print("----- select_equals() ----")
    select_equals()

    print("----- select_not_equals() ----")
    select_not_equals

    print("----- select_greater_than() ----")
    select_greater_than()

    print("----- select_less_than() ----")
    select_less_than()

    print("----- select_like() ----")
    select_like()

    print("----- select_not_like() ----")
    select_not_like()

    print("----- select_contains() ----")
    select_contains()

    print("----- select_startswith() ----")
    select_startswith()

    print("----- select_endswith() ----")
    select_endswith()

    print("----- select_in_() ----")
    select_in_()

    print("----- select_not_in() ----")
    select_not_in()

    print("----- select_with_python_and() ----")
    select_with_python_and()

    print("----- select_and_() ----")
    select_and_()

    print("----- select_multiple_where() ----")
    select_multiple_where()

    print("----- select_with_python_or() ----")
    select_with_python_or()

    print("----- select_or_() ----")
    select_or_()

    print("----- select_partial() ----")
    select_partial()

    print("----- END ----")


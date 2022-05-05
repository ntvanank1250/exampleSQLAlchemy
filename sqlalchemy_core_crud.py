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

def show_metadata():
    for t in metadata.sorted_tables:
        print(f"Table {t.name}:")
        for c in t.columns:
            print(f"{c} ({c.type})")

def do_insert():
    stmt = employees.insert().values(
        LastName='Collins',
        FirstName='Arnold',
        BirthDate='2000-01-31')
    new_id = 0
    with engine.connect() as con:
        result = con.execute(stmt)
        new_id = result.inserted_primary_key['Id']
        print(f"New ID: {new_id}")
    return new_id

def select_by_id(id):
    stmt = employees.select().where(employees.c.Id == id)
    with engine.connect() as con:
        result = con.execute(stmt).first()
        if result:
            print(result)
        else:
            print(f"no roW found with Id =={id}")

def do_update(id):
    stmt = employees.update().values(
        FirstName="Michael"
    ).where(employees.c.Id == id)

    with engine.connect() as con:
        con.execute(stmt)

def do_delete(id):
    stmt = employees.delete().where(employees.c.Id == id)

    with engine.connect() as con:
        con.execute(stmt)

def statement_infos():
    stmt = employees.select(employees.c.LastName, employees.c.FirstName).where(employees.c.Id == 30)
    print(f"statement with placeholder: \n{str(stmt)}")
    print(f"\nparams: \n{str(stmt.compile().params)}")

if __name__=='__main__':
    print("----- show_metadata() ----")
    show_metadata()

    print("----- do_insert() ----")
    id= do_insert()

    print("----- select_by_id) ----")
    select_by_id(id)

    print("----- do_update() ----")
    do_update(id)
    select_by_id(id)

    print("----- do_delete() ----")
    do_delete(id)
    select_by_id(id)

    print("----- END ----")

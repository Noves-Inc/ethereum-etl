import click
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import SQLAlchemyError

def generate_create_table_statements(metadata):
    """
    Generates the CREATE TABLE statements for all tables in the given metadata.
    """
    create_statements = []
    for table in metadata.tables.values():
        create_table_stmt = CreateTable(table).compile(dialect=dialect())
        create_statements.append(str(create_table_stmt))
    return create_statements

@click.command()
@click.argument("connection_string")
def main(connection_string):
    """
    Connects to a PostgreSQL database using the CONNECTION_STRING and creates tables
    defined in the metadata of the provided schema file.
    """
    try:
        from ethereumetl.streaming.postgres_tables import metadata
        
        # Generate SQL statements
        create_statements = generate_create_table_statements(metadata)
        
        # Connect to the database
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            transaction = conn.begin()  # Explicitly begin a transaction
            try:
                for stmt in create_statements:
                    print(f"Executing:\n{stmt}\n")
                    conn.execute(text(stmt))  # Ensure proper execution
                transaction.commit()  # Explicitly commit the transaction
                print("All tables created successfully.")
            except Exception as e:
                transaction.rollback()  # Rollback on error
                raise e  # Reraise the exception for visibility
    except SQLAlchemyError as e:
        click.echo(f"Database error: {e}", err=True)
    except ImportError as e:
        click.echo(f"Error importing schema file: {e}", err=True)

if __name__ == "__main__":
    main()

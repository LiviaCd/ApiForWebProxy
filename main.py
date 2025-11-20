from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from uuid import UUID, uuid4
from cassandra.cluster import Cluster

# -----------------------------
# Cassandra connection
# -----------------------------
cluster = Cluster(["127.0.0.1"])   # dacă folosești Docker: acesta este corect
session = cluster.connect("techframer")

app = FastAPI()

# -----------------------------
# Pydantic model
# -----------------------------
class Book(BaseModel):
    title: str
    author: str
    year: int

# -----------------------------
# CREATE
# -----------------------------
@app.post("/books")
def create_book(book: Book):
    book_id = uuid4()
    session.execute(
        """
        INSERT INTO books (id, title, author, year)
        VALUES (%s, %s, %s, %s)
        """,
        (book_id, book.title, book.author, book.year)
    )
    return {"id": book_id, "message": "Book created"}

# -----------------------------
# READ ALL
# -----------------------------
@app.get("/books")
def get_books():
    rows = session.execute("SELECT * FROM books")
    return [
        {
            "id": str(r.id),
            "title": r.title,
            "author": r.author,
            "year": r.year
        }
        for r in rows
    ]

# -----------------------------
# READ BY ID
# -----------------------------
@app.get("/books/{book_id}")
def get_book(book_id: UUID):
    row = session.execute(
        "SELECT * FROM books WHERE id = %s",
        (book_id,)
    ).one()

    if not row:
        raise HTTPException(status_code=404, detail="Book not found")

    return {
        "id": str(row.id),
        "title": row.title,
        "author": row.author,
        "year": row.year
    }

# -----------------------------
# UPDATE
# -----------------------------
@app.put("/books/{book_id}")
def update_book(book_id: UUID, book: Book):
    result = session.execute(
        "SELECT id FROM books WHERE id = %s", (book_id,)
    ).one()

    if not result:
        raise HTTPException(status_code=404, detail="Book not found")

    session.execute(
        """
        UPDATE books
        SET title = %s, author = %s, year = %s
        WHERE id = %s
        """,
        (book.title, book.author, book.year, book_id)
    )

    return {"message": "Book updated"}

# -----------------------------
# DELETE
# -----------------------------
@app.delete("/books/{book_id}")
def delete_book(book_id: UUID):
    result = session.execute(
        "SELECT id FROM books WHERE id = %s", (book_id,)
    ).one()

    if not result:
        raise HTTPException(status_code=404, detail="Book not found")

    session.execute(
        "DELETE FROM books WHERE id = %s", (book_id,))

    return {"message": "Book deleted"}

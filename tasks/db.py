"""Conexão com o PostgreSQL (banco `vendas`)."""
import psycopg2

from tasks.config import PG


def conectar():
    return psycopg2.connect(**PG)

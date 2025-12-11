
# Parques de Estacionamento

## Tabelas da base de dados

### Parque

| Coluna           |  Tipo sugerido | Observações    |
| ---------------- | -------------: | -------------- |
| <u>codParque</u> |          `INT` | **PK**         |
| designacao       | `VARCHAR(200)` | nome do parque |

### Registo

| Coluna            | Tipo sugerido | Observações                    |
| ----------------- | ------------: | ------------------------------ |
| <u>codRegisto</u> |         `INT` | **PK**                         |
| entrada           |    `DATETIME` | instante de entrada            |
| saida             |    `DATETIME` | instante de saída              |
| codParque_fk      |         `INT` | **FK → Parque(codParque)**     |
| codigo_fk         |         `INT` | **FK → Identificador(codigo)** |

### Identificador

| Coluna        | Tipo sugerido | Observações                 |
| ------------- | ------------: | --------------------------- |
| <u>codigo</u> |         `INT` | **PK**                      |
| matricula_fk  | `VARCHAR(20)` | **FK → Viatura(matricula)** |

### Viatura

| Coluna           | Tipo sugerido | Observações               |
| ---------------- | ------------: | ------------------------- |
| <u>matricula</u> | `VARCHAR(20)` | **PK**                    |
| codTipo          |         `INT` | código do tipo de viatura |
| nif_fk           |     `CHAR(9)` | NIF do proprietário       |

### Tabela de Preços

| Coluna              |  Tipo sugerido | Observações                |
| ------------------- | -------------: | -------------------------- |
| <u>codParque_fk</u> |          `INT` | **FK → Parque(codParque)** |
| <u>codTipo</u>      |          `INT` | código do tipo de viatura  |
| preco               | `DECIMAL(8,2)` | preço por unidade de tempo |


---

**Nota**: a entidade TipoViatura poderia ser guardada em base dados, mas estou a assumir que serão poucos, logo não vale a pena guardar em base de dados.

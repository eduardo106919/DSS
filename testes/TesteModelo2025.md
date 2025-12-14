
# DSS Teste Modelo 2025 - Parte 3

## Questão 5

Qual a frase que melhor descreve a seguinte expressão OCL?
```
context Encomenda::processItem(item: Item) : void
post: self.items->forall(i | i <> item)
```

- Depois de o método ter sido executado, o item passado como parâmetro não deve existir nos items da encomenda


## Questão 6

Considere o modelo de domínio apresentado anteriormente. Qual das seguintes frases é uma **descrição verdadeira da informação apresentada no diagrama**?

- O pedido de defesa de um aluno tem que estar assinado por um orientador.


## Questão 7

Considere que está a desenvolver um editor gráfico, em que existem diversas formas geométricas que é possível adicionar à imagem em edição e sobre as quais é possível calcular o perímetro, a área, etc. Relembre os princípios de design orientado a objetos discutidos nas aulas, analise as afirmações que se seguem e indique as que considera verdadeiras:

- O Open/Closed Principle, ao dizer que a arquitetura do editor deve permitir que a adição de nova funcionalidade seja feita através da extensão de entidades existentes, em vez de através da sua modificação, implica que é preferível definir uma hierarquia de classes para representar as formas geométricas, em vez de ter uma única classe com um atributo que diz qual o tipo da forma geométrica.

- O Single Layer of Abstraction Principle, ao dizer que não devemos misturar diferentes níveis de abstração no mesmo código, promove que os atributos de cada classe sejam apenas acedidos dentro da própria classe.


## Questão 8

Considere que se vai desenvolver uma arquitetura a partir do modelo de domínio apresentado anteriormente. Sabendo que cada documento é identificado por um número e que se pretende que os alunos tenham acesso fácil ao documento que submetem, quais das seguintes afirmações considera verdadeira?

- A classe Aluno deverá ter uma associação para o documento que o aluno submeteu.
- A classe Aluno deverá ter um atributo com o número do documento que o aluno submeteu.

## Questão 9

Quais dos seguintes diagramas UML são os mais adequados para modelar a forma como um sistema reage a eventos externos?

- Diagrama de Máquinas de Estado
- Diagrama de Sequência
- Diagrama de Actividades

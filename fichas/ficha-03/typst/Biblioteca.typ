#set text(font: "PT Sans", lang: "pt", size: 11pt)

#let uc_spec(
  nome: "",
  descricao: "",
  cenarios: "",
  pre_condicoes: "",
  pos_condicoes: "",
  fluxo_normal: (),
  fluxos_alternativos: (), 
  fluxos_excecao: ()       
) = {
  block(
    width: 100%,
    stroke: 0.5pt + luma(150),
    radius: 4pt,
    inset: 12pt,
    
    align(left)[
      #stack(
        spacing: 10pt,
        text(weight: "bold", size: 1.2em, blue.darken(40%))[Caso de Uso: #nome],
        line(length: 100%, stroke: 0.5pt + luma(200)),
        
        grid(
          columns: (0.8fr, 3fr),
          row-gutter: 8pt,
          [*Descrição:*], [#descricao],
          [*Cenários:*], [#cenarios],
          [*Pré-Condições:*], [#pre_condicoes],
          [*Pós-Condições:*], [#pos_condicoes],
        ),
        
        text(weight: "bold", fill: green.darken(20%))[Fluxo Normal:],
        enum(..fluxo_normal),
        
        if fluxos_alternativos.len() > 0 [
          #text(weight: "bold", fill: orange.darken(20%))[Fluxos Alternativos:]
          #for fa in fluxos_alternativos [
            #block(inset: (left: 10pt), spacing: 6pt)[
              *Condição:* #fa.condicao (Passo #fa.passo)
              #enum(numbering: n => str(fa.passo) + "." + str(n), ..fa.fluxo)
            ]
          ]
        ],
        
        if fluxos_excecao.len() > 0 [
          #text(weight: "bold", fill: red.darken(20%))[Fluxos de Exceção:]
          #for fe in fluxos_excecao [
            #block(inset: (left: 10pt), spacing: 6pt)[
              *Condição:* #fe.condicao (Passo #fe.passo)
              #enum(numbering: n => str(fe.passo) + "." + str(n), ..fe.fluxo)
            ]
          ]
        ]
      )
    ]
  )
}

#uc_spec(
  nome: "Validar Credenciais de Utente",
  descricao: "Sistema valida credenciais de utente para efetuar requisições, entregas e pagamento de multas",
  cenarios: "Cenário 1, 2, 3, 4 e 5",
  pre_condicoes: "True",
  pos_condicoes: "Credenciais autenticadas",
  fluxo_normal: (
    [Funcionário apresenta número e nome do utente],
    [Sistema valida número e nome de utente],
  ),
  fluxos_excecao: (
    (
      condicao: "Número e nome inexistentes",
      passo: "2",
      fluxo: (
        [Sistema indica que utente é inexistente],
      )
    ),
  )
)

#uc_spec(
  nome: "Pagar Multas",
  descricao: "Funcionário regista o pagamento de multas pelo atraso na entrega de livros requisitados",
  cenarios: "Cenário 2 e 5",
  pre_condicoes: "Funcionário autenticado",
  pos_condicoes: "Multas Pagas",
  fluxo_normal: (
    [Sistema calcula valor de multas a pagar],
    [Funcionário confirma pagamento de multas],
    [Sistema imprime comprovativo de pagamento de multas],
  ),
  fluxos_excecao: (
    (
      condicao: "Utente não paga as multas",
      passo: "2",
      fluxo: (
        [Sistema regista que utente não pagou as multas],
      )
    ),
  )
)

#uc_spec(
  nome: "Registar Entrega de Livro",
  descricao: "Funcionário regista a entrefa de livros requisitados por utentes",
  cenarios: "Cenário 4 e 5",
  pre_condicoes: "Funcionário autenticado",
  pos_condicoes: "Devolução do livro registado e estado do livro alterado para 'Disponível'",
  fluxo_normal: (
    [`<<include>>` Validar Credenciais de Utente],
    [Funcionário fornece identificação do livro],
    [Sistema valida identificação do livro],
    [Sistema valida que entrega está dentro do prazo],
    [Sistema regista a entrega do livro],
    [Sistema altera estado do livro para 'Disponível'],
    [Sistema gera comprovativo de devolução]
  ),
  fluxos_alternativos: (
    (
      condicao: "Entrega está fora de prazo",
      passo: "4",
      fluxo: (
        [`<<include>>` Pagar Multas],
        [Regressa a 5]
      )
    ),
  ),
  fluxos_excecao: (
    (
      condicao: "Livro não pertence à biblioteca",
      passo: "3",
      fluxo: (
        [Sistema informa que livro não pertence à biblioteca],
      )
    ),
  )
)

#uc_spec(
  nome: "Registar Requisição de Livro",
  descricao: "Funcionário regista a requisição de um livro por parte de um utente",
  cenarios: "Cenário 1, 2 e 3",
  pre_condicoes: "Funcionário autenticado",
  pos_condicoes: "Registo de requisição adicionado ao sistema e estado do livro alterado para 'Requisitado'",
  fluxo_normal: (
    [`<<include>>` Validar Credenciais de Utente],
    [Sistema valida que utente não tem multas a pagar],
    [Funcionário indica código do livro],
    [Sistema valida disponibilidade do livro para ser requisitado],
    [Sistema regista requisição do livro],
    [Sistema altera estado do livro para 'Requisitado'],
    [Sistema calcula data de devolução do livro],
    [Sistema gera comprovativo da requisição]
  ),
  fluxos_alternativos: (
    (
      condicao: "Utente tem multas a pagar",
      passo: "2",
      fluxo: (
        [`<<include>>` Pagar Multas],
        [Sistema prolonga a data de devolução dos livros por entregar],
        [Regressa a 3]
      )
    ),
  ),
  fluxos_excecao: (
    (
      condicao: "Livro não pode ser requisitado",
      passo: "4",
      fluxo: (
        [Sistema informa que livro não pode ser requisitado],
      )
    ),
  )
)


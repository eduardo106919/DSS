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
  nome: "Iniciar Sessão",
  descricao: "Utilizador autentica-se na rede social",
  cenarios: "Cenário 1",
  pre_condicoes: "Utilizador não autenticado",
  pos_condicoes: "Utlizador autenticado e com acesso à rede social",
  fluxo_normal: (
    [Utilizador apresenta o seu identificador e senha],
    [Sistema valida identificador e senha],
    [Sistema apresenta uma lista de publicações feitas ou partilhadas pelas pessoas que segue],
  ),
  fluxos_excecao: (
    (
      condicao: "Credenciais inválidas",
      passo: "2",
      fluxo: (
        [Sistema informa que credenciais são inválidas],
      )
    ),
  )
)

#uc_spec(
  nome: "Adicionar Publicação",
  descricao: "Utlizador adiciona uma publicação",
  cenarios: "Cenário 2",
  pre_condicoes: "Utilizador autenticado",
  pos_condicoes: "Sistema tem mais uma publicação",
  fluxo_normal: (
    [Utilizador adiciona o conteúdo da publicação],
    [Sistema valida o conteúdo da publicação],
    [Sistema adiciona a nova publicação],
  ),
  fluxos_excecao: (
    (
    condicao: "Conteúdo da aplicação inválido",
    passo: "2",
    fluxo: (
      [Sistema informa que conteúdo da publicação quebra as regras],
      )
    ),
  )
)

#pagebreak()

#uc_spec(
  nome: "Comentar Publicação",
  descricao: "Utilizador comenta uma publicação de outro utilizador",
  cenarios: "Cenário 1",
  pre_condicoes: "Utilizador autenticado",
  pos_condicoes: "Comentário adicionado a publicação",
  fluxo_normal: (
    [Utlizador seleciona uma publicação],
    [Utilizador escreve comentário],
    [Sistema valida conteúdo do comentário],
    [Sistema adiciona comentário à publicação],
  ),
  fluxos_alternativos: (
    (
      condicao: "Utilizador reage com emoji",
      passo: "2",
      fluxo: (
        [Utilizador escolhe emoji],
        [Regressa a 4],
      )
    ),
  ),
  fluxos_excecao: (
    (
      condicao: "Conteúdo do comentário inválido",
      passo: "3",
      fluxo: (
        [Sistema informa que conteúdo do comentário quebra as regras],
      )
    ),
  )
)
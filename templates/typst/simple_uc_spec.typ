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
  nome: "Nome do Use Case",
  descricao: "Descrição breve do Use Case",
  cenarios: "Identificar cenários em que o Use Case está presente",
  pre_condicoes: "Condições necessárias para a execução do Use Case",
  pos_condicoes: "Condições a satisfazer no fim do Use Case",
  fluxo_normal: (
    [],
    [],
    [],
    []
  ),
  fluxos_alternativos: (
    (
      condicao: "Condição que deu origem ao fluxo alternativo",
      passo: "1",
      fluxo: (
        [],
        [],
        [],
      )
    ),
  ),
  fluxos_excecao: (
    (
      condicao: "Condição que deu origem ao fluxo de exceção",
      passo: "2",
      fluxo: (
        [],
        [],
      )
    ),
  )
)

#uc_spec(
  nome: "Nome do Use Case",
  descricao: "Descrição breve do Use Case",
  cenarios: "Identificar cenários em que o Use Case está presente",
  pre_condicoes: "Condições necessárias para a execução do Use Case",
  pos_condicoes: "Condições a satisfazer no fim do Use Case",
  fluxo_normal: (
    [],
    [],
    [],
    []
  )
)

#pagebreak()

#uc_spec(
  nome: "Levantar Dinheiro",
  descricao: "Cliente levanta quantia da máquina.",
  cenarios: "Cenários 1 e 2 (O João levanta €60 com cartão; O João levanta €10 com MB way).",
  pre_condicoes: "Sistema tem notas.",
  pos_condicoes: "Cliente tem quantia desejada e saldo da conta foi actualizado.",
  fluxo_normal: (
    [Cliente apresenta cartão e PIN.],
    [Máquina MB valida acesso e pede operação.],
    [Cliente indica que pretende levantar dada quantia.],
    [Máquina MB pergunta se quer talão.],
    [Cliente responde que não.],
    [Máquina MB devolve cartão, fornece notas e actualiza saldo da conta.],
    [Cliente retira cartão e notas.],
  ),
  fluxos_alternativos: (
    (
      condicao: "Cliente quer autenticar-se com reconhecimento facial.",
      passo: "1",
      fluxo: (
        [Cliente apresenta cartão e pede reconhecimento facial.],
        [Máquina recolhe imagem para validação de acesso.],
        [Regressa a 2.],
      )
    ),
    (
      condicao: "Cliente quer talão.",
      passo: "5",
      fluxo: (
        [Cliente responde que sim.],
        [Máquina MB devolve cartão, notas e talão.],
        [Cliente retira cartão, notas e talão e actualiza saldo da conta.],
      )
    )
  ),
  fluxos_excecao: (
    (
      condicao: "O sistema não encontra correspondência para os dados inseridos.",
      passo: "3",
      fluxo: (
        [Máquina MB avisa sobre PIN inválido e devolve cartão.],
        [Cliente retira cartão.],
      )
    ),
  )
)

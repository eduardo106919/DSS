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
  nome: "Realizar Check-in",
  descricao: "Participante efetua o check-in para participar no evento",
  cenarios: "Cenário 1",
  pre_condicoes: "Check-in não efetuado",
  pos_condicoes: "Presença registada e Participante particia no evento",
  fluxo_normal: (
    [Participante apresenta o seu nome],
    [Sistema procura nome na lista de inscritos],
    [Sistema seleciona inscrição],
    [Sistema imprime badge do Participante],
    [Sistema regista presença do Participante]
  ),
  fluxos_alternativos: (
    (
      condicao: "Existem vários inscritos com o mesmo nome",
      passo: "2",
      fluxo: (
        [Participante indica instituição de origem e email],
        [Regressa a 3],
      )
    ),
    (
      condicao: "Participante não está inscrito",
      passo: "3",
      fluxo: (
        [Participante apresenta o seu nome, email e instituição de origem],
        [Sistema adiciona inscrição],
        [Regressa a 3]
      )
    )
  ),
  fluxos_excecao: (
    (
      condicao: "Participante desiste",
      passo: "3.1",
      fluxo: (
        [Participante vai se embora],
      )
    ),
  )
)


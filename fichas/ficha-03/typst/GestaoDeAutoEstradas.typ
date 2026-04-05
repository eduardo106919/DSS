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
  nome: "Consultar Extrato",
  descricao: "Cliente consulta extrato mensal dos seus identificadores",
  cenarios: "Cenário 2",
  pre_condicoes: "Cliente autenticado",
  pos_condicoes: "Extrato consultado",
  fluxo_normal: (
    [Cliente indica data de passagem],
    [Sistema apresenta lista de passagens],
    [Sistema apresenta extrato da conta],
  ),
  fluxos_alternativos: (
    (
      condicao: "Passagem identificada por imagem",
      passo: "2",
      fluxo: (
        [Sistema apresenta imagem capturada],
        [Sistema apresenta cobrança da passagem],
        [Cliente valida identificação e cobrança],
        [Regressa a 3]
      )
    ),
  ),
  fluxos_excecao: (
    (
      condicao: "Identificação incorreta",
      passo: "2.3",
      fluxo: (
        [Cliente comunica erro de identificação],
      )
    ),
  )
)

#uc_spec(
  nome: "Registar Passagem",
  descricao: "Pórtico regista passagem de veiatura na portagem",
  cenarios: "Cenário 1",
  pre_condicoes: "True",
  pos_condicoes: "Registo de passgem adicionado ao sistema",
  fluxo_normal: (
    [Viatura entra na auto-estrada],
    [Pórtico lê o identificador da viatura],
    [Pórtico comunica passagem ao sistema],
    [Sistema regista identificador, hora e local de passagem]
  ),
  fluxos_alternativos: (
    (
      condicao: "Identificador inoperacional",
      passo: "2",
      fluxo: (
        [Pórtico captura imagem de viatura],
        [Pórtico envia imagem ao sistema],
        [Sistema regista imagem para identificação da viatura]
      )
    ),
    (
      condicao: "Número de identificador inválido",
      passo: "4",
      fluxo: (
        [Sistema comunica erro de identificador inexistente],
        [Pórtico captura imagem de viatura],
        [Pórtico envia imagem ao sistema],
        [Sistema regista imagem para identificação da viatura]
      )
    ),
  )
)


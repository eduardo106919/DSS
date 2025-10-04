/*
 *  DISCLAIMER: Este código foi criado para discussão e edição durante as aulas práticas de DSS, representando
 *  uma solução em construção. Como tal, não deverá ser visto como uma solução canónica, ou mesmo acabada.
 *  É disponibilizado para auxiliar o processo de estudo. Os alunos são encorajados a testar adequadamente o
 *  código fornecido e a procurar soluções alternativas, à medida que forem adquirindo mais conhecimentos.
 */
package uminho.dss.turmas3l.ui;

import uminho.dss.turmas3l.business.*;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * Exemplo de interface em modo texto.
 *
 * @author DSS
 * @version 20230915
 */
public class TextUI {
    // O model tem a 'lógica de negócio'.
    private ITurmasFacade model;

    // Scanner para leitura
    private Scanner scin;

    /**
     * Construtor.
     * Cria os menus e a camada de negócio.
     */
    public TextUI() {

        this.model = new TurmasFacade();
        scin = new Scanner(System.in);
    }

    /**
     * Executa o menu principal e invoca o método correspondente à opção seleccionada.
     */
    public void run() {
        System.out.println("Bem vindo ao Sistema de Gestão de Turmas!");
        this.menuPrincipal();
        System.out.println("Até breve...");
    }

    // Métodos auxiliares - Estados da UI

    /**
     * Estado - Menu Principal
     */
    private void menuPrincipal() {
        Menu menu = new Menu(new String[]{
                "Operações sobre Alunos",
                "Operações sobre Turmas",
                "Operações sobre Salas",
                "Adicionar Aluno a Turma",
                "Remover Aluno de Turma",
                "Listar Alunos de Turma"
        });

        // Registar pré-condições das transições
        menu.setPreCondition(2, ()->this.model.haSalas());
        menu.setPreCondition(4, ()->this.model.haAlunos() && this.model.haTurmas());
        menu.setPreCondition(5, ()->this.model.haTurmasComAlunos());

        // Registar os handlers das transições
        menu.setHandler(1, ()->gestaoDeAlunos());
        menu.setHandler(2, ()->gestaoDeTurmas());
        menu.setHandler(3, ()->gestaoDeSalas());
        menu.setHandler(4, ()->adicionarAlunoATurma());
        menu.setHandler(5, ()->removerAlunoDeTurma());
        menu.setHandler(6, ()->listarAlunosDaTurma());

        menu.run();
    }

    /**
     *  Estado - Gestão de Alunos
     */
    private void gestaoDeAlunos() {
        Menu menu = new Menu("Gestão de Alunos", new String[]{
                "Adicionar Aluno",
                "Consultar Aluno",
                "Listar Alunos"
        });

        // Registar os handlers
        menu.setHandler(1, ()->adicionarAluno());
        menu.setHandler(2, ()->consultarAluno());
        menu.setHandler(3, ()->listarAlunos());

        menu.run();
    }

    /**
     * Estado - Gestão de Turmas
     */
    private void gestaoDeTurmas() {
        Menu menu = new Menu("Gestão de Turmas", new String[]{
                "Adicionar Turma",
                "Mudar Sala à Turma",
                "Listar Turmas"
        });

        // Registar os handlers - utilizando referências para métodos em vez de expressões lamdas
        menu.setHandler(1, this::adicionarTurma);
        menu.setHandler(2, this::mudarSalaDeTurma);
        menu.setHandler(3, this::listarTurmas);

        menu.run();
    }

    private void gestaoDeSalas() {
        Menu menu = new Menu("Gestão de Salas", new String[] {
                "Adicionar Sala",
                "Remover Sala",
                "Listas Salas"
        });

        menu.setHandler(1, this::adicionarSala);
        menu.setHandler(2, this::removerSala);
        menu.setHandler(3, this::listarSalas);

        menu.run();
    }

    /**
     *  Estado - Adicionar Aluno
     */
    private void adicionarAluno() {
        try {
            System.out.println("Número da novo aluno: ");
            String num = scin.nextLine();
            if (!this.model.existeAluno(num)) {
                System.out.println("Nome da novo aluno: ");
                String nome = scin.nextLine();
                System.out.println("Email da novo aluno: ");
                String email = scin.nextLine();
                this.model.adicionaAluno(new Aluno(num, nome, email));
                System.out.println("Aluno adicionado");
            } else {
                System.out.println("Esse número de aluno já existe!");
            }
        }
        catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     *  Estado - Consultar Aluno
     */
    private void consultarAluno() {
        try {
            System.out.println("Número a consultar: ");
            String num = scin.nextLine();
            if (this.model.existeAluno(num)) {
                System.out.println(this.model.procuraAluno(num).toString());
            } else {
                System.out.println("Esse número de aluno não existe!");
            }
        }
        catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     *  Estado - Listar Alunos
     */
    private void listarAlunos() {
        try {
            System.out.println(this.model.getAlunos().toString());
        }
        catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     *  Estado - Adicionar Turma
     */
    private void adicionarTurma() {
        try {
            System.out.println("Número da turma: ");
            String tid = scin.nextLine();
            if (!this.model.existeTurma(tid)) {
                System.out.println("Sala: ");
                String sala = scin.nextLine();
                if (model.existeSala(sala)) {
                    this.model.adicionaTurma(new Turma(tid, model.getSala(sala)));
                    System.out.println("Turma adicionada");
                } else {
                    System.out.println("Esse número de sala não existe");
                }
            } else {
                System.out.println("Esse número de turma já existe!");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     *  Estado - Mudar Sala de Turma
     */
    private void mudarSalaDeTurma() {
        try {
            System.out.println("Número da turma: ");
            String tid = scin.nextLine();
            if (this.model.existeTurma(tid)) {
                System.out.println("Sala: ");
                String sala = scin.nextLine();
                if (model.existeSala(sala)) {
                    this.model.alteraSalaDeTurma(tid, sala);
                    System.out.println("Sala da turma alterada");
                } else {
                    System.out.println("Esse número de sala não existe!");
                }
            } else {
                System.out.println("Esse número de turma não existe!");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     *  Estado - Listar Turmas
     */
    private void listarTurmas() {
        try {
            System.out.println(this.model.getTurmas().toString());
        }
        catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
    }

    private void adicionarSala() {
        try {
            System.out.println("Número da sala: ");
            String sid = scin.nextLine();
            if (!model.existeSala(sid)) {
                System.out.println("Edificio: ");
                String edificio = scin.nextLine();
                System.out.println("Capacidade: ");
                int capacidade = scin.nextInt();
                // limpar buffer
                scin.nextLine();

                model.adicionaSala(new Sala(sid, edificio, capacidade));
                System.out.println("Sala adicionada!");
            } else {
                System.out.println("Esse número de sala já existe");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void removerSala() {
        try {
            System.out.println("Número da sala: ");
            String sid = scin.nextLine();
            if (model.existeSala(sid)) {
                model.removeSala(sid);
                System.out.println("Sala eliminada!");
            } else {
                System.out.println("Esse número de sala não existe");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void listarSalas() {
        System.out.println(model.getSalas().toString());
    }

    /**
     *  Estado - Adicionar Aluno a Turma
     */
    private void adicionarAlunoATurma() {
        try {
            System.out.println("Número da turma: ");
            String tid = scin.nextLine();
            if (this.model.existeTurma(tid)) {
                System.out.println("Número do aluno: ");
                String num = scin.nextLine();
                if (this.model.existeAluno(num)) {
                    this.model.adicionaAlunoTurma(tid, num);
                    System.out.println("Aluno adicionado à turma");
                } else {
                    System.out.println("Esse número de aluno não existe!");
                }
            } else {
                System.out.println("Esse número de turma não existe!");
            }
        }
        catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     *  Estado - Remover Aluno de Turma
     *
     *  Exemplo de menu dinâmico
     */
    private void removerAlunoDeTurma() {
        try {
            System.out.println("Número da turma: ");
            String tid = scin.nextLine();
            if (this.model.existeTurma(tid)) {
                // Obter os alunos
                List<Aluno> ca = this.model.getAlunos(tid).stream()
                        .collect(Collectors.toList());
                // Construit o menu de alunos
                List<String> lalunos = ca.stream()
                        .map(a->(a.getNumero()+"-"+a.getNome()))
                        .collect(Collectors.toList());
                Menu menu = new Menu("Aluno a remover...", lalunos);

                // As opções do menu começam em 1, ms as lisats em 0!!1
                for(int i=1; i<= lalunos.size(); i++) {
                    int idx = i;
                    menu.setHandler(i, ()->{
                        this.model.removeAlunoTurma(tid, ca.get(idx-1).getNumero());
                        menu.setPreCondition(idx, ()->false);   // 'remover' aluno do menu
                    });
                }
                menu.run();
            } else {
                System.out.println("Esse número de turma não existe!");
            }
        }
        catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     *  Estado - Listar Alunos da Turma
     */
    private void listarAlunosDaTurma() {
        try {
            System.out.println("Número da turma: ");
            String tid = scin.nextLine();
            System.out.println(this.model.getAlunos(tid).toString());
        }
        catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
    }
}

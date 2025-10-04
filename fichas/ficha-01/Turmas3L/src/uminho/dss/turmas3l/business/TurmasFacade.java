/*
 *  DISCLAIMER: Este código foi criado para discussão e edição durante as aulas práticas de DSS, representando
 *  uma solução em construção. Como tal, não deverá ser visto como uma solução canónica, ou mesmo acabada.
 *  É disponibilizado para auxiliar o processo de estudo. Os alunos são encorajados a testar adequadamente o
 *  código fornecido e a procurar soluções alternativas, à medida que forem adquirindo mais conhecimentos.
 */
package uminho.dss.turmas3l.business;

import uminho.dss.turmas3l.data.AlunoDAO;
import uminho.dss.turmas3l.data.SalaDAO;
import uminho.dss.turmas3l.data.TurmaDAO;

import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * Versão muito simplificada de um Facade para a turma.
 *
 * @author DSS
 * @version 20230915
 */
public class TurmasFacade implements ITurmasFacade {

    private Map<String,Turma> turmas;
    private Map<String,Aluno> alunos;
    private Map<String,Sala> salas;

    public TurmasFacade() {
        this.salas = SalaDAO.getInstance();
        this.turmas = TurmaDAO.getInstance();
        this.alunos = AlunoDAO.getInstance();
    }

    /**
     * Método que devolve todos os alunos registados.
     *
     * @return todos os alunos registados
     */
    @Override
    public Collection<Aluno> getAlunos() {
        return new ArrayList<>(this.alunos.values());
    }

    /**
     * Método que remove um aluno da turma.
     *
     * @param tid id da turma
     * @param num número do aluno a remover
     */
    @Override
    public void removeAlunoTurma(String tid, String num) {
        Turma t = this.turmas.get(tid);
        t.removeAluno(num);
        this.turmas.put(tid, t);  // Necessário fazer put para actualizar a BD.
    }

    /**
     * Método que devolve os alunos de uma turma.
     *
     * Transforma a lista de Números de aluno que vem do TurmaDAO em alunos .
     *
     * @param tid id da turma
     * @return alunos da turma
     */
    @Override
    public Collection<Aluno> getAlunos(String tid) {
        return turmas.get(tid).getAlunos().stream().map(na -> alunos.get(na)).collect(toList());
    }

    /**
     * @return todas as turmas
     */
    @Override
    public Collection<Turma> getTurmas() {
        return turmas.values();
    }

    /**
     * @param a aluno a adicionar
     */
    @Override
    public void adicionaAluno(Aluno a) {
        this.alunos.put(a.getNumero(), a);
    }

    /**
     * @param t turma a dicionar
     */
    @Override
    public void adicionaTurma(Turma t) {
        this.turmas.put(t.getId(), t);
    }

    /**
     * @param tid id da turma que vai mudar de sala
     * @param sid id da nova sala da turma
     */
    @Override
    public void alteraSalaDeTurma(String tid, String sid) {
        Turma t = turmas.get(tid);
        t.setSala(salas.get(sid));
        turmas.put(t.getId(), t); // Para actualizar a BD!
    }

    /**
     * @param tid id da turma a procurar
     * @return true se a turma existe
     */
    @Override
    public boolean existeTurma(String tid) {
        return turmas.containsKey(tid);
    }

    /**
     * @param num número do aluno a procurar
     * @return true se o aluno existe
     */
    @Override
    public Aluno procuraAluno(String num) {
        return this.alunos.get(num);
    }

    /**
     * @param num número do aluno a procurar
     * @return true se o aluno existe
     */
    @Override
    public boolean existeAluno(String num) {
        return this.alunos.containsKey(num);
    }

    /**
     * Método que adiciona um aluno à turma.
     *
     * NOTA 1: Falta garantir que aluno existe
     * NOTA 2: Uma vez que o modelo relacional só permite que o aluno esteja numa turma, este será retirado da
     * turma anterior, se estiver numa.
     *
     * @param tid id da turma onde se vai colocar o aluno
     * @param num número do aluno
     */
    @Override
    public void adicionaAlunoTurma(String tid, String num) {
        Turma t = this.turmas.get(tid);
        t.adiciona(num);
        turmas.put(tid, t);
    }

    /**
     * @param tid Turma a pesqueisar
     * @param num Número de aluno a pesquisar
     * @return True se aluno existe na turma
     */
    @Override
    public boolean existeAlunoEmTurma(String tid, String num) {
        return this.turmas.get(tid).existeAluno(num);
    }

    /**
     * Método que verifica se há alunos no sistema
     * @return true se há alunos registados
     */
    @Override
    public boolean haAlunos() {
        return !this.alunos.isEmpty();
    }

    /**
     * Método que verifica se há turmas no sistema
     * @return true se há turmas registadas
     */
    @Override
    public boolean haTurmas() {
        return !this.turmas.isEmpty();
    }

    /**
     * Método que verifica se há turmas com alunos registados
     * @return true se há alguma turma com alunos registados
     */
    @Override
    public boolean haTurmasComAlunos() {
        return this.turmas.values().stream().anyMatch(t -> !t.getAlunos().isEmpty());
    }

    @Override
    public boolean existeSala(String num) {
        return salas.containsKey(num);
    }

    @Override
    public void adicionaSala(Sala s) {
        salas.put(s.getNumero(), s);
    }

    @Override
    public void removeSala(String num) {
        salas.remove(num);
    }

    @Override
    public Sala getSala(String num) {
        return salas.get(num);
    }

    @Override
    public Collection<Sala> getSalas() {
        return salas.values();
    }

    @Override
    public boolean haSalas() {
        return !salas.isEmpty();
    }

}

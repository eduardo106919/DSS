package uminho.dss.turmas3l.data;

import uminho.dss.turmas3l.business.Aluno;

import java.sql.*;
import java.util.*;

/**
 *
 * @author Eduardo Freitas Fernandes
 */
public class AlunoDAO implements Map<String, Aluno> {

    // variáveis de classe

    /** instância única de AlunoDAO */
    private static AlunoDAO singleton = null;


    // construtores

    /**
     * Construtor de AlunoDAO que cria a tabela na base de dados
     */
    private AlunoDAO() {
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS alunos (" +
                    "Num varchar(10) NOT NULL PRIMARY KEY," +
                    "Nome varchar(45) DEFAULT NULL," +
                    "Email varchar(45) DEFAULT NULL," +
                    "Turma varchar(10), foreign key(Turma) references turmas(Id))";
            stm.executeUpdate(sql);
        } catch(SQLException e) {
            // erro a criar tabela
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }
    }
    
    
    // métodos de classe

    /**
     * Devolve a instância única de AlunoDAO
     * @return instância única de AlunoDAO
     */
    public static AlunoDAO getInstance() {
        if (AlunoDAO.singleton == null)
            AlunoDAO.singleton = new AlunoDAO();
        return AlunoDAO.singleton;
    }


    // métodos de instância

    @Override
    public int size() {
        int s = 0;

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery("SELECT COUNT(*) FROM alunos")) {
            if (rs.next())
                s = rs.getInt(1);
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return s;
    }

    @Override
    public boolean isEmpty() {
        return this.size() == 0;
    }

    @Override
    public boolean containsKey(Object o) throws NullPointerException, ClassCastException {
        if (o == null)
            throw new NullPointerException();
        if (!(o instanceof String))
            throw new ClassCastException();

        boolean out = false;

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("SELECT Num FROM alunos WHERE Num = ?")) {
            pstm.setString(1, o.toString());
            try (ResultSet rs = pstm.executeQuery()) {
                out = rs.next();
            }
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public boolean containsValue(Object o) throws NullPointerException, ClassCastException {
        if (o == null)
            throw new NullPointerException();
        if (!(o instanceof Aluno))
            throw new ClassCastException();

        Aluno a = (Aluno) o;
        return this.containsKey(a.getNumero());
    }

    @Override
    public Aluno get(Object o) throws NullPointerException, ClassCastException {
        if (o == null)
            throw new NullPointerException();
        if (!(o instanceof String))
            throw new ClassCastException();

        Aluno out = null;

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("SELECT * FROM alunos WHERE Num = ?")) {
            pstm.setString(1, o.toString());
            try (ResultSet rs = pstm.executeQuery()) {
                // chave encontrada na base de dados
                if (rs.next())
                    out = new Aluno(rs.getString("Num"), rs.getString("Nome"), rs.getString("Email"));
            }
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public Aluno put(String s, Aluno aluno) throws NullPointerException {
        if (s == null || aluno == null)
            throw new NullPointerException();

        Aluno out = this.remove(s);

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("INSERT INTO alunos (Num, Nome, Email) VALUES (?, ?, ?)")) {
            pstm.setString(1, s);
            pstm.setString(2, aluno.getNome());
            pstm.setString(3, aluno.getEmail());
            pstm.executeUpdate();
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public Aluno remove(Object o) throws NullPointerException, ClassCastException {
        if (o == null)
            throw new NullPointerException();
        if (!(o instanceof String))
            throw new ClassCastException();

        Aluno out = null;
        String num = o.toString();

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("SELECT * FROM alunos WHERE Num = ?")) {
            pstm.setString(1, num);
            // procurar aluno
            try (ResultSet rs = pstm.executeQuery()) {
                // aluno encontrado
                if (rs.next()) {
                    out = new Aluno(rs.getString("Num"), rs.getString("Nome"), rs.getString("Email"));

                    // eliminar aluno encontrado
                    try (PreparedStatement del = conn.prepareStatement("DELETE FROM alunos WHERE Num = ?")) {
                        del.setString(1, num);
                        del.executeUpdate();
                    }
                }
            }
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public void putAll(Map<? extends String, ? extends Aluno> map) {
        for (Aluno a: map.values())
            this.put(a.getNumero(), a);
    }

    @Override
    public void clear() {
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement()) {
            stm.executeUpdate("TRUNCATE alunos");
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }
    }

    @Override
    public Set<String> keySet() {
        Set<String> out = new HashSet<>();

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery("SELECT Num FROM alunos")) {
            while (rs.next())
                out.add(rs.getString(1));
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public Collection<Aluno> values() {
        Collection<Aluno> out = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery("SELECT * FROM alunos")) {
            while (rs.next())
                out.add(new Aluno(rs.getString("Num"), rs.getString("Nome"), rs.getString("Email")));
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public Set<Entry<String, Aluno>> entrySet() {
        Set<Entry<String, Aluno>> out = new HashSet<>();
        Entry<String, Aluno> et;

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery("SELECT * FROM alunos")) {
            while (rs.next()) {
                et = new AbstractMap.SimpleEntry<>(
                        rs.getString("Num"),
                        new Aluno(rs.getString("Num"), rs.getString("Nome"), rs.getString("Email"))
                );
                out.add(et);
            }
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

}

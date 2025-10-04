package uminho.dss.turmas3l.data;

import uminho.dss.turmas3l.business.Sala;

import java.sql.*;
import java.util.*;

public class SalaDAO implements Map<String, Sala> {

    // variáveis de classe

    private static SalaDAO singleton = null;


    // construtores

    private SalaDAO() {
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS salas (" +
                    "Num varchar(10) NOT NULL PRIMARY KEY," +
                    "Edificio varchar(45) DEFAULT NULL," +
                    "Capacidade int(4) DEFAULT 0)";
            stm.executeUpdate(sql);
        } catch(SQLException e) {
            // erro a criar tabela
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }
    }


    // métodos de classe

    /**
     * Devolve a instância única de SalaDAO
     * @return instância única de SalaDAO
     */
    public static SalaDAO getInstance() {
        if (SalaDAO.singleton == null)
            SalaDAO.singleton = new SalaDAO();
        return SalaDAO.singleton;
    }

    // métodos de instância

    @Override
    public int size() {
        int s = 0;

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery("SELECT COUNT(*) FROM salas")) {
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
             PreparedStatement pstm = conn.prepareStatement("SELECT Num FROM salas WHERE Num = ?")) {
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
        if (!(o instanceof Sala))
            throw new ClassCastException();

        Sala a = (Sala) o;
        return this.containsKey(a.getNumero());
    }

    @Override
    public Sala get(Object o) throws NullPointerException, ClassCastException {
        if (o == null)
            throw new NullPointerException();
        if (!(o instanceof String))
            throw new ClassCastException();

        Sala out = null;

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("SELECT * FROM salas WHERE Num = ?")) {
            pstm.setString(1, o.toString());
            try (ResultSet rs = pstm.executeQuery()) {
                // chave encontrada na base de dados
                if (rs.next())
                    out = new Sala(rs.getString("Num"), rs.getString("Edificio"), rs.getInt("Capacidade"));
            }
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public Sala put(String s, Sala sala) throws NullPointerException {
        if (s == null || sala == null)
            throw new NullPointerException();

        Sala out = this.remove(s);

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("INSERT INTO salas (Num, Edificio, Capacidade) VALUES (?, ?, ?)")) {
            pstm.setString(1, s);
            pstm.setString(2, sala.getEdificio());
            pstm.setInt(3, sala.getCapacidade());
            pstm.executeUpdate();
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public Sala remove(Object o) throws NullPointerException, ClassCastException {
        if (o == null)
            throw new NullPointerException();
        if (!(o instanceof String))
            throw new ClassCastException();

        Sala out = null;
        String num = o.toString();

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("SELECT * FROM salas WHERE Num = ?")) {
            pstm.setString(1, num);
            // procurar sala
            try (ResultSet rs = pstm.executeQuery()) {
                // sala encontrada
                if (rs.next()) {
                    out = new Sala(rs.getString("Num"), rs.getString("Edificio"), rs.getInt("Capacidade"));

                    // atualizar turmas
                    try (PreparedStatement turmas = conn.prepareStatement("UPDATE turmas SET Sala = ? WHERE Sala = ?")) {
                        turmas.setNull(1, Types.VARCHAR);
                        turmas.setString(2, num);
                        turmas.executeUpdate();
                    }

                    // eliminar sala encontrada
                    try (PreparedStatement del = conn.prepareStatement("DELETE FROM salas WHERE Num = ?")) {
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
    public void putAll(Map<? extends String, ? extends Sala> map) {
        for (Sala s: map.values())
            this.put(s.getNumero(), s);
    }

    @Override
    public void clear() {
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement()) {
            stm.executeUpdate("TRUNCATE salas");
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
             ResultSet rs = stm.executeQuery("SELECT Num FROM salas")) {
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
    public Collection<Sala> values() {
        Collection<Sala> out = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery("SELECT * FROM salas")) {
            while (rs.next())
                out.add(new Sala(rs.getString("Num"), rs.getString("Edificio"), rs.getInt("Capacidade")));
        } catch(SQLException e) {
            // database error
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return out;
    }

    @Override
    public Set<Entry<String, Sala>> entrySet() {
        Set<Entry<String, Sala>> out = new HashSet<>();
        Entry<String, Sala> et;

        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery("SELECT * FROM salas")) {
            while (rs.next()) {
                et = new AbstractMap.SimpleEntry<>(
                        rs.getString("Num"),
                        new Sala(rs.getString("Num"), rs.getString("Edificio"), rs.getInt("Capacidade"))
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

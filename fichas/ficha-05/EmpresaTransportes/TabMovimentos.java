
import java.util.Collection;
import java.util.ArrayList;

public class TabMovimentos {

    private Collection<Movimento> movs;

    public void add(Movimento m) {
        this.movs.add(m);
    }

    public Collection<String> comTantasParagens(int n) {
        Collection<String> res = new ArrayList();

        for (Movimento m : this.movs) {
            Bilhete b = m.getBil();
            int t = b.getTotParagens();

            if (t == n) {
                String num = b.getNumero();
                res.add(num);
            }
        }

        return res;
    }

    public Collection<Bilhete> inicioEm(String local) {
        Collection<Bilhete> res = new ArrayList();

        for (Movimento m : this.movs) {
            String l = m.getLocalEntrada();

            if (local.equals(l)) {
                Bilhete b = m.getBil();

                res.add(b);
            }
        }

        return res;
    }

}
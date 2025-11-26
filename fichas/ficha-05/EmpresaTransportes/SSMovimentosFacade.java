import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SSMovimentosFacade implements ISSMovimentos {

    private TabMovimentos tabMovs;

    public Collection<String> comTantasParagens(int n) {
        List<String> cods = new ArrayList<>();
        List<Movimento> movs = this.tabMovs.getMovs();

        for (Movimento m: movs) {
            Bilhete b = m.getBil();
            int t = b.getTotParagens();
            if (t == n) {
                String c = b.getNumero();
                cods.add(c);
            }
        }

        return cods;
    }

    public Collection<Bilhete> inicioEm(String local) {
        List<Bilhete> bils = new ArrayList<>();
        List<Movimento> movs = this.tabMovs.getMovs();

        for (Movimento m: movs) {
            String l = m.getLocalEntrada();
            if (l.equals(local)) {
                Bilhete b = m.getBil();
                bils.add(b);
            }
        }

        return bils;
    }
}

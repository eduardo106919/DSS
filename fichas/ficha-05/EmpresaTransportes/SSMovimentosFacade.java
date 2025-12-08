
import java.util.Collection;

public class SSMovimentosFacade implements ISSMovimentos {

    private TabMovimentos tabMovs;

    public Collection<String> comTantasParagens(int n) {
        Collection<String> out = this.tabMovs.comTantasParagens(n);
        return out;
    }
    
    public Collection<Bilhete> inicioEm(String local) {
        Collection<Bilhete> out = this.tabMovs.inicioEm(local);
        return out;
    }

}
import java.util.List;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException {
             DB db=new DB();
             if(!db.open())
                 System.out.println("oops");
               db.querySongMetaData();
             db.close();
    }
}
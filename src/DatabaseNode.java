import java.io.*;
import java.net.*;
import java.util.*;

public class DatabaseNode {
    private int tcpport; // numer portu TCP dla klientów
    private Map<Integer, Integer> data; // pary klucz-wartość przechowywane przez węzeł
    private Set<String> connections; // zestaw połączeń z innymi węzłami
    private ServerSocket serverSocket; // socket dla klientów
    private Socket socket; // socket dla połączeń z innymi węzłami

    public DatabaseNode(int tcpport, int key, int value) {
        this.tcpport = tcpport;
        data = new HashMap<>();
        data.put(key, value);
        connections = new HashSet<>();
    }

    // metoda dodająca nowe połączenie z innym węzłem
    public void addConnection(String address, int port) {
        System.out.println("Dodaję połączenie do " + address + ":" + port);
        connections.add(address + ":" + port);
    }

    // metoda obsługująca odczyt danych
    public int read(int key) {
        return data.getOrDefault(key, -1);
    }

    // metoda obsługująca zapis danych
    public void write(int key, int value) {
        data.put(key, value);
    }

    public void start() throws IOException {
// utworzenie socketu dla klientów
        serverSocket = new ServerSocket(tcpport);
        System.out.println("Węzeł rozpoczął nasłuchiwanie na porcie " + tcpport);
        // utworzenie wątku do obsługi połączeń z innymi węzłami
        new Thread(() -> {
            for (String connection : connections) {
                String[] parts = connection.split(":");
                String address = parts[0];
                int port = Integer.parseInt(parts[1]);
                try {
                    socket = new Socket(address, port);
                    // Obsługa połączenia z innym węzłem (np. wymiana danych)
                    // tutaj można dodać odpowiedni kod
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

// utworzenie wątku do obsługi połączeń od klientów
        new Thread(() -> {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    String inputLine;
// oczekiwanie na polecenie od klienta
                    while ((inputLine = in.readLine()) != null) {
                        String[] parts = inputLine.split(" ");
                        String command = parts[0];
                        if (command.equals("read")) {
// odczyt danych
                            int key = Integer.parseInt(parts[1]);
                            int value = read(key);
                            out.println(value);
                        } else if (command.equals("write")) {
// zapis danych
                            int key = Integer.parseInt(parts[1]);
                            int value = Integer.parseInt(parts[2]);
                            write(key, value);
                            out.println("OK");
                        } else {
// nieznane polecenie
                            out.println("ERROR");
                        }
                    }
                    in.close();
                    out.close();
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static void main(String[] args) throws IOException {
// parsowanie argumentów wywołania programu
        int tcpport = 0;
        int key = 0;
        int value = 0;
        Set<String> connections = new HashSet<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-tcpport":
                    tcpport = Integer.parseInt(args[++i]);
                    break;
                case "-record":
                    String[] record = args[++i].split(":");
                    key = Integer.parseInt(record[0]);
                    value = Integer.parseInt(record[1]);
                    break;
                case "-connect":
                    connections.add(args[++i]);
                    break;
                default:
                    System.out.println("Nieznany argument: " + args[i]);
                    break;
            }
        }
// utworzenie węzła sieci
        DatabaseNode node = new DatabaseNode(tcpport, key, value);
        for (String connection : connections) {
            String[] parts = connection.split(":");
            node.addConnection(parts[0], Integer.parseInt(parts[1]));
        }
// uruchomienie węzła
        node.start();
    }
}

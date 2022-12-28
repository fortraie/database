import java.io.*;
import java.net.*;
import java.util.*;


public class DatabaseNode {


    private int tcpPort; // Port tworzonego węzła (dla połączenia z klientami)
    private ServerSocket serverSocket; // Socket tworzonego węzła (dla połączenia z klientami)
//    private Socket socket; // Socket tworzonego węzła (dla połączenia z innymi węzłami)

    private Map<Integer, Integer> data; // Przechowywane pary klucz-wartość
    private Set<String> connections; // Połączenia z innymi węzłami


    public DatabaseNode(int tcpPort, int key, int value, Set<String> connections) {
        this.tcpPort = tcpPort;
        this.connections = connections;

        data = new HashMap<>();
        data.put(key, value);
    }


    public void addConnection(String connection) {
        connections.add(connection);
    }


    public int getValue(int key) {
        return data.getOrDefault(key, -1);
    }


    public void newRecord(int key, int value) {
        data.put(key, value);
    }


    public void start() throws IOException {


        // Utworzenie socketu, obsługującego połączenia klientów
        serverSocket = new ServerSocket(tcpPort);
        System.out.println("Węzeł " + tcpPort + " rozpoczął nasłuchiwanie.");


        // Utworzenie wątku, obsługującego zapytania od połączonych klientów
        new Thread(() -> {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    String inputLine;

                    // Warunek wykluczający kontynuację wykonania programu przed otrzymaniem zapytania
                    while ((inputLine = in.readLine()) != null) {
                        String[] parts = inputLine.split(" ");
                        String command = parts[0];
                        System.out.println("Węzeł " + tcpPort + " otrzymał zapytanie: " + inputLine + " od klienta " + clientSocket.getInetAddress().getCanonicalHostName() + ":" + clientSocket.getLocalPort() + ".");

                        switch (command) {
                            // Odczyt danych klucz-wartość (według wartości klucza)
                            case "get-value" -> {
                                int key = Integer.parseInt(parts[1]);
                                int value = getValue(key);

                                // Wysłanie zapytania do połączonych węzłów w sytuacji braku danych w lokalnej bazie
                                if (value == -1) {
                                    for (String connection : connections) {
                                        String response = forward(connection, inputLine);
                                        if (Integer.parseInt(response) != -1) {
                                            value = Integer.parseInt(response);
                                            break;
                                        }
                                    }
                                }
                                System.out.println("Węzeł " + tcpPort + " odczytał wartość " + value + " dla klucza " + key + ".");
                                out.println(value);
                            }
                            // Utworzenie nowej pary klucz-wartość
                            case "new-record" -> {
                                int key = Integer.parseInt(parts[1]);
                                int value = Integer.parseInt(parts[2]);
                                newRecord(key, value);
                                System.out.println("Węzeł " + tcpPort + " utworzył nowy rekord: " + key + " " + value + ".");
                                out.println("OK");
                            }
                            // Komunikat o błędzie w zapytaniu
                            default -> {
                                out.println("ERROR");
                            }
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


    private String forward(String connection, String inputLine) {
        String[] parts = connection.split(":");
        String address = parts[0];
        int port = Integer.parseInt(parts[1]);
        System.out.println("Węzeł " + tcpPort + " wysyła zapytanie " + inputLine + " do węzła " + address + ":" + port + ".");

        try {
            Socket connSocket = new Socket(address, port);
            PrintWriter connOut = new PrintWriter(connSocket.getOutputStream(), true);
            BufferedReader connIn = new BufferedReader(new InputStreamReader(connSocket.getInputStream()));

            connOut.println(inputLine);
            String response = connIn.readLine();

            connOut.close();
            connIn.close();
            connSocket.close();

            return response;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }


    public static void main(String[] args) throws IOException {


        int tcpPort = 0;
        int key = 0;
        int value = 0;
        Set<String> connections = new HashSet<>();

        // Parsowanie argumentów wywołania programu z tablicy args
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-tcpport" -> {
                    tcpPort = Integer.parseInt(args[++i]);
                }
                case "-record" -> {
                    String[] record = args[++i].split(":");
                    key = Integer.parseInt(record[0]);
                    value = Integer.parseInt(record[1]);
                }
                case "-connect" -> {
                    connections.add(args[++i]);

                    // Dodanie informacji o połączeniu do węzła, z którym jest nawiązane połączenie
                    for (String connection : connections) {

                    }
                }
                default -> {
                    System.out.println("Nieznany argument: " + args[i]);
                }
            }
        }

        // Utworzenie nowego węzła
        DatabaseNode node = new DatabaseNode(tcpPort, key, value, connections);
        node.start();


    }
}

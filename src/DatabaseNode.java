import java.io.*;
import java.net.*;
import java.util.*;


public class DatabaseNode {


    private int tcpPort; // Port tworzonego węzła (dla połączenia z klientami)
    private ServerSocket serverSocket; // Socket tworzonego węzła (dla połączenia z klientami)

    private int key; // Klucz
    private int value; // Wartość
    private Set<String> connections; // Połączenia z innymi węzłami

    private final String LOCAL_ADDRESS;



    public DatabaseNode(int tcpPort, int key, int value, Set<String> connections) {
        this.tcpPort = tcpPort;
        this.key = key;
        this.value = value;
        this.connections = connections;

        try {
            this.LOCAL_ADDRESS = InetAddress.getLocalHost().getHostAddress() + ":" + tcpPort;
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


    public String getLOCAL_ADDRESS() {
        return LOCAL_ADDRESS;
    }


    public void setConnections(Set<String> connections) {
        this.connections = connections;
    }

    public void start() throws IOException {


        // Utworzenie socketu, obsługującego połączenia klientów
        serverSocket = new ServerSocket(tcpPort);
        System.out.printf("[%s] Rozpoczęcie nasłuchiwania z wartością %s:%s.\n", LOCAL_ADDRESS, key, value);



        // Utworzenie wątku, obsługującego zapytania od połączonych klientów
        new Thread(() -> {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    String inputLine;
                    String clientAddress = "";

                    // Warunek wykluczający kontynuację wykonania programu przed otrzymaniem zapytania
                    while ((inputLine = in.readLine()) != null) {
                        String[] parts = inputLine.split(" ");
                        String command = parts[0];
                        clientAddress = clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort();
                        System.out.printf("[%s <- %s] Otrzymanie zapytania: %s.\n", LOCAL_ADDRESS, clientAddress, inputLine);

                        switch (command) {
                            // Zastąpienie danych wartości dla zadanego klucza
                            case "set-value" -> {
                                String[] subParts = parts[1].split(":");
                                int inKey = Integer.parseInt(subParts[0]);
                                boolean isSuccess = false;

                                if (inKey == key) {
                                    value = Integer.parseInt(subParts[1]);
                                    isSuccess = true;
                                } else {
                                    for (String connection : connections) {
                                        String response = forward(connection, "get-record");
                                        String[] responseParts = response.split(":");
                                        if (Integer.parseInt(responseParts[0]) == inKey) {
                                            forward(connection, "set-value " + parts[1]);
                                            isSuccess = true;
                                            break;
                                        }
                                    }
                                }

                                if (!isSuccess) {
                                    System.out.printf("[%s -> %s] Zmiana wartości nie powiodła się.\n", LOCAL_ADDRESS, clientAddress);
                                    out.println("ERROR");
                                } else {
                                    System.out.printf("[%s -> %s] Zmiana wartości na %s.\n", LOCAL_ADDRESS, value, clientAddress);
                                    out.println("OK");
                                }
                            }
                            // Odczyt danych klucz-wartość (według wartości klucza)
                            case "get-value" -> {
                                int inKey = Integer.parseInt(parts[1]);
                                int outValue = -1;

                                if (inKey == key) {
                                    outValue = value;
                                } else {
                                    // Wysłanie zapytania do połączonych węzłów w sytuacji braku danych w lokalnej bazie
                                    for (String connection : connections) {
                                        String response = forward(connection, "get-record");
                                        String[] responseParts = response.split(":");
                                        if (Integer.parseInt(responseParts[0]) == inKey) {
                                            outValue = Integer.parseInt(responseParts[1]);
                                            break;
                                        }
                                    }
                                }

                                if (outValue == -1) {
                                    System.out.printf("[%s -> %s] Odczytanie wartości nie powiodło się.\n", LOCAL_ADDRESS, clientAddress);
                                    out.println("ERROR");
                                } else {
                                    System.out.printf("[%s -> %s] Odczytanie wartości: %s.\n", LOCAL_ADDRESS, clientAddress, outValue);
                                    out.println(outValue);
                                }



                            }
                            // Zwrócenie adresu i numeru portu węzła, na którym przechowywany jest rekord o zadanym kluczu.
                            case "find-key" -> {
                                int inKey = Integer.parseInt(parts[1]);
                                String outNode = "";

                                if (inKey == key) {
                                    outNode = LOCAL_ADDRESS;
                                } else {
                                    // Wysłanie zapytania do połączonych węzłów w sytuacji braku danych w lokalnej bazie
                                    for (String connection : connections) {
                                        String response = forward(connection, "get-record");
                                        String[] responseParts = response.split(":");
                                        if (Integer.parseInt(responseParts[0]) == inKey) {
                                            outNode = connection;
                                            break;
                                        }
                                    }
                                }

                                if (outNode.equals("")) {
                                    System.out.printf("[%s -> %s] Nie znaleziono klucza: %s.\n", LOCAL_ADDRESS, clientAddress, inKey);
                                    out.println("ERROR");
                                } else {
                                    System.out.printf("[%s -> %s] Odczytanie węzła: %s.\n", LOCAL_ADDRESS, clientAddress, outNode);
                                    out.println(outNode);
                                }
                            }
                            // Zwrócenie największej wartości ze wszystkich węzłów
                            case "get-max" -> {
                                Map<Integer, Integer> values = new HashMap<>();
                                values.put(key, value);

                                for (String connection : connections) {
                                    String response = forward(connection, "get-record");
                                    String[] responseParts = response.split(":");
                                    values.put(Integer.parseInt(responseParts[0]), Integer.parseInt(responseParts[1]));
                                }

                                int maxKey = Collections.max(values.keySet());
                                int maxValue = values.get(maxKey);

                                System.out.printf("[%s -> %s] Odczytanie wartości: %s.\n", LOCAL_ADDRESS, clientAddress, maxValue);
                                out.println(maxKey + ":" + maxValue);
                            }
                            // Zwrócenie najmniejszej wartości ze wszystkich węzłów
                            case "get-min" -> {
                                Map<Integer, Integer> values = new HashMap<>();
                                values.put(key, value);

                                for (String connection : connections) {
                                    String response = forward(connection, "get-record");
                                    String[] responseParts = response.split(":");
                                    values.put(Integer.parseInt(responseParts[0]), Integer.parseInt(responseParts[1]));
                                }

                                int minKey = Collections.min(values.keySet());
                                int minValue = values.get(minKey);

                                System.out.printf("[%s -> %s] Odczytanie wartości: %s.\n", LOCAL_ADDRESS, clientAddress, minValue);
                                out.println(minKey + ":" + minValue);
                            }
                            // Utworzenie nowej pary klucz-wartość
                            case "new-record" -> {
                                String[] subParts = parts[1].split(":");
                                key = Integer.parseInt(subParts[0]);
                                value = Integer.parseInt(subParts[1]);
                                System.out.printf("[%s] Utworzenie nowej pary klucz-wartość: %s, %s.\n", LOCAL_ADDRESS, key, value);
                                out.println("OK");
                            }
                            // Zwrócenie wartości klucza i wartości
                            case "get-record" -> {
                                System.out.printf("[%s -> %s] Odczytanie wartości: %s:%s.\n", LOCAL_ADDRESS, clientAddress, key, value);
                                out.println(key + ":" + value);
                            }
                            // Wyłączanie węzła
                            case "terminate" -> {
                                System.out.printf("[%s] Wyłączenie węzła.\n", LOCAL_ADDRESS);
                                out.println("OK");
                                for (String connection : connections) {
                                    System.out.printf("[%s -> %s] Wyłączenie węzła.\n", LOCAL_ADDRESS, connection);
                                    forward(connection, "remove-connection "  + LOCAL_ADDRESS);
                                }
                                System.exit(0);
                            }
                            // Dodanie nowego połączenia
                            case "add-connection" -> {
                                String connection = parts[1];
                                connections.add(connection);
                                System.out.printf("[%s] Dodanie połączenia: %s.\n", LOCAL_ADDRESS, connection);
                                out.println("OK");
                            }
                            // Usunięcie dotychczasowego połączenia
                            case "remove-connection" -> {
                                String connection = parts[1];
                                connections.remove(connection);
                                System.out.printf("[%s] Usunięcie połączenia: %s.\n", LOCAL_ADDRESS, connection);
                                out.println("OK");
                            }
                            // Zwracanie informacji o hoście
                            case "get-host" -> {
                                System.out.printf("[%s -> %s] Odczytanie hosta: %s.\n", LOCAL_ADDRESS, clientAddress, LOCAL_ADDRESS);
                                out.println(LOCAL_ADDRESS);
                            }
                            // Komunikat o błędzie w zapytaniu
                            default -> {
                                System.out.printf("[%s -> %s] Błąd w zapytaniu: %s.\n", LOCAL_ADDRESS, clientAddress, inputLine);
                                out.println("ERROR");
                            }
                        }
                        break;
                    }
                    System.out.printf("[%s -> %s] Zakończenie połączenia.\n", LOCAL_ADDRESS, clientAddress);
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
        System.out.printf("[%s -> %s] Przekierowanie zapytania: %s.\n", LOCAL_ADDRESS, connection, inputLine);

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
                }
                default -> {
                    System.out.println("Błąd w argumentach wywołania programu.");
                    System.exit(1);
                }
            }
        }

        // Utworzenie nowego węzła
        DatabaseNode node = new DatabaseNode(tcpPort, key, value, connections);
        node.start();

        Set<String> connectionsFixed = new HashSet<>();
        
        // Dodanie informacji o połączeniu do węzła, z którym jest nawiązane połączenie
        for (String connection : connections) {
            String connectionAddress = node.forward(connection, "get-host");
            connectionsFixed.add(connectionAddress);
        }


        node.setConnections(connectionsFixed);

        for (String connectionFixed : connectionsFixed) {
            node.forward(connectionFixed, "add-connection " + node.getLOCAL_ADDRESS());
        }


    }
}

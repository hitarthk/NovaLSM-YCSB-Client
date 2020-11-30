package com.yahoo.ycsb.db;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class NovaClient {

    public static class Sock {
        Socket socket;
        DataInputStream in;
        BufferedOutputStream out;
    }

    private static final int BUF_SIZE = 1024 * 1024 * 2; // 1 MB.
    private static AtomicInteger gThreadId = new AtomicInteger(0);

    private List<Sock> sockets = new ArrayList<>();
    private byte[] socketBuffer = new byte[BUF_SIZE];
    private int tid = 0;

    private final List<String> servers;
    private final boolean debug;
    private int sock_read_pivot = 0;

    public static class ReturnValue {
        String getValue = null;
        int configId = 0;
    }

    public List<String> getServers() {
        return servers;
    }

    public void close() {
        try {
            for (int i = 0; i < sockets.size(); i++) {
                sockets.get(i).socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int bytesToInt(byte[] buf, int offset) {
        int x = 0;
        int i = offset;
        while (buf[i] != '!') {
            char c = (char) buf[i];
            x = (c - '0') + x * 10;
            i++;
        }
        return x;
    }

    private long bytesToLong(byte[] buf) {
        long x = 0;
        while (buf[sock_read_pivot] != '!') {
            char c = (char) buf[sock_read_pivot];
            x = (c - '0') + x * 10;
            sock_read_pivot++;
        }
        sock_read_pivot++;
        return x;
    }

    private int longToBytes(byte[] buf, int offset, long x) {
        int len = 0, p = offset;
        do {
            buf[offset + len] = (byte) ((x % 10) + '0');
            x = x / 10;
            len++;
        } while (x != 0);
        int q = offset + len - 1;
        byte temp;
        while (p < q) {
            temp = buf[p];
            buf[p] = buf[q];
            buf[q] = temp;
            p++;
            q--;
        }
        buf[offset + len] = '!';
        return len + 1;
    }

    private void copyString(byte[] buf, int offset, String value) {
        for (int i = 0; i < value.length(); i++) {
            buf[offset + i] = (byte) (value.charAt(i));
        }
    }

    private int read(InputStream in) {
        try {
            // Read at much as possible for the first time.
            int len = -1;
            int readBytes = in.read(socketBuffer);
            int count = 0;
            for (int i = 0; i < readBytes; i++) {
                if (socketBuffer[i] == '!' && len == -1) {
                    len = bytesToInt(socketBuffer, 0);
                    continue;
                }
                if (len != -1) {
                    socketBuffer[count] = socketBuffer[i];
                    count++;
                }
            }

            if (len == -1) {
                // read one byte at a time until we know the length.
                do {
                    socketBuffer[readBytes] = (byte) in.read();
                    readBytes++;
                } while (socketBuffer[readBytes - 1] != '!');
                len = bytesToInt(socketBuffer, 0);
            }

            // read the remaining bytes.
            while (count < len) {
                int cnt = in.read(socketBuffer, count, (len - count));
                count += cnt;
            }
            return len;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private int readPlainText(InputStream in, char terminateChar) {
        try {
            // Read at much as possible for the first time.
            int readBytes = in.read(socketBuffer);
            if (socketBuffer[readBytes - 1] == terminateChar) {
                return readBytes;
            }
            // read one byte at a time until we know the length.
            do {
                socketBuffer[readBytes] = (byte) in.read();
                readBytes++;
            } while (socketBuffer[readBytes - 1] != terminateChar);
            return readBytes;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public NovaClient(List<String> servers, boolean debug) {
        tid = gThreadId.incrementAndGet();
        this.debug = debug;
        this.servers = servers;
        for (int i = 0; i < servers.size(); i++) {
            String host = servers.get(i);
            String ip = host.split(":")[0];
            int port = Integer.parseInt(host.split(":")[1]);
            try {
                if (debug) {
                    System.out.println("Connecting to " + ip + ":" + port);
                }
                Socket sock = null;
                while (sock == null) {
                    try {
                        sock = new Socket(ip, port);
                        sock.setTcpNoDelay(false);
                    } catch (Exception e) {
                        Thread.sleep(5000);
                    }
                }
                Sock so = new Sock();
                so.socket = sock;
                so.in = new DataInputStream(sock.getInputStream());
                so.out = new BufferedOutputStream(sock.getOutputStream());
                sockets.add(so);
            } catch (Exception e) {
                System.exit(-1);
            }
        }
    }

    public ReturnValue get(int clientConfigId, String key, int serverId) {
        ReturnValue v = new ReturnValue();
        try {
            sock_read_pivot = 0;
            int intKey = Integer.parseInt(key);

            Sock sock = getSock(serverId);
            if (debug) {
                System.out.println(String.format("Get: tid:%d sid:%d key:%d", tid, serverId, intKey));
            }
            socketBuffer[0] = 'g';
            int cfgSize = longToBytes(socketBuffer, 1, clientConfigId);

            int size = longToBytes(socketBuffer, 1 + cfgSize, intKey);
            socketBuffer[cfgSize + size + 1] = '\n';
            sock.out.write(socketBuffer, 0, cfgSize + size + 2);
            sock.out.flush();

            int len = readPlainText(sock.in, '\n');
            v.configId = (int) bytesToLong(socketBuffer);
            if (debug) {
                System.out.println(String.format("Hit: tid:%d sid:%d key:%d size:%d", tid, serverId, intKey, len - 1));
            }
            if (v.configId != clientConfigId) {
                return v;
            }
            int valueSize = (int) bytesToLong(socketBuffer);
            assert valueSize > 0;
            v.getValue = new String(socketBuffer, sock_read_pivot, (int) valueSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return v;
    }

    public ReturnValue scan(int clientConfigId, String key, int nrecords, int server_id, List<String> keys,
                            List<String> values) {
        sock_read_pivot = 0;
        int sid = server_id;
        int intKey = Integer.parseInt(key);
        ReturnValue v = new ReturnValue();
        try {
            Sock sock = getSock(sid);
            if (debug) {
                System.out.println(String.format("Scan: tid:%d sid:%d key:%d", tid, sid, intKey));
            }
            socketBuffer[0] = 'r';
            int size = 1;
            size += longToBytes(socketBuffer, size, clientConfigId);
            size += longToBytes(socketBuffer, size, intKey);
            size += longToBytes(socketBuffer, size, nrecords);
            socketBuffer[size] = '\n';
            size++;
            sock.out.write(socketBuffer, 0, size);
            sock.out.flush();
            int len = readPlainText(sock.in, '\n');

            if (debug) {
                System.out.println(len);
                for (int i = 0; i < len; i++) {
                    System.out.print((char) socketBuffer[i]);
                }
                System.out.println();
            }

            if (len > 0) {
                v.configId = (int) bytesToLong(socketBuffer);
                if (v.configId != clientConfigId) {
                    return v;
                }

                long keySize = 0;
                long valueSize = 0;
                do {
                    keySize = bytesToLong(socketBuffer);
                    assert keySize > 0;
                    String rkey = new String(socketBuffer, sock_read_pivot, (int) keySize);
                    sock_read_pivot += keySize;

                    valueSize = bytesToLong(socketBuffer);
                    assert valueSize > 0;
                    String rvalue = new String(socketBuffer, sock_read_pivot, (int) (valueSize));
                    sock_read_pivot += valueSize;

                    if (debug) {
                        System.out.println(rkey + " " + rvalue);
                    }

                    if (debug) {
                        // System.out.println(String.format("key-%s value-%d", rkey,
                        // rvalue.length()));
                    }
                    keys.add(rkey);
                    values.add(rvalue);
                    if (socketBuffer[sock_read_pivot] == '\n') {
                        break;
                    }
                } while (sock_read_pivot < len);
                assert sock_read_pivot + 1 == len;
            }
            if (debug) {
                System.out.println(String.format("Scan: key:%s nrecords:%d len:%d", key, nrecords, len));
            }
            sock_read_pivot = 0;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return v;
    }

    private Sock getSock(int serverId) {
        return sockets.get(serverId);
    }

    public ReturnValue put(int clientConfigId, String key, String value, int serverId) {
        ReturnValue v = new ReturnValue();
        try {
            sock_read_pivot = 0;
            Sock sock = getSock(serverId);
            int intKey = Integer.parseInt(key);
            socketBuffer[0] = 'p';
            int size = 1;
            size += longToBytes(socketBuffer, size, clientConfigId);
            size += longToBytes(socketBuffer, size, intKey);
            size += longToBytes(socketBuffer, size, value.length());
            copyString(socketBuffer, size, value);
            size += value.length();
            socketBuffer[size] = '\n';
            size++;
            sock.out.write(socketBuffer, 0, size);
            sock.out.flush();

            if (debug) {
                System.out.println(
                        String.format("Put: tid:%d sid:%d key:%d size:%d", tid, serverId, intKey, value.length()));
            }

            int response = readPlainText(sock.in, '\n');
//			String val;
//			for (int i = 0; i < response; i++) {
//				System.out.print((char) socketBuffer[i]);
//			}

            v.configId = (int) bytesToLong(socketBuffer);
            socketBuffer[0] = 'a';
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return v;
    }

    public ReturnValue changeConfig(int serverId) {
        ReturnValue v = new ReturnValue();
        sock_read_pivot = 0;
        try {
            Sock sock = getSock(serverId);
            socketBuffer[0] = 'b';
            socketBuffer[1] = '\n';
            sock.out.write(socketBuffer, 0, 2);
            sock.out.flush();
            int response = readPlainText(sock.in, '\n');
            v.configId = (int) bytesToLong(socketBuffer);
            socketBuffer[0] = 'a';
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return v;
    }

    public ReturnValue requestSourceToSendData(int serverId, char migration_phase) {
        ReturnValue v = new ReturnValue();
        sock_read_pivot = 0;
        try {
            Sock sock = getSock(serverId);
            socketBuffer[0] = 'H';
            socketBuffer[1] = (byte)migration_phase;
            socketBuffer[2] = '\n';
            sock.out.write(socketBuffer, 0, 3);
            sock.out.flush();
            System.out.println("Before read");
            int readLength = readPlainText(sock.in, '\n');
            v.configId = (int) bytesToLong(socketBuffer);
            socketBuffer[0] = 'a';
        } catch (Exception e) {
            System.out.println("Error Initiating src driven migration");
            e.printStackTrace();
            System.exit(-1);
        }
        return v;
    }

    public ReturnValue getLogRecordsReplayed(int serverId, long ltcFragmentId) {
        ReturnValue v = new ReturnValue();
        try {
            sock_read_pivot = 0;
            Sock sock = getSock(serverId);
            socketBuffer[0] = 'l';
            int size = 1;
            size += longToBytes(socketBuffer, 1, ltcFragmentId);
            socketBuffer[size++] = '\n';
            System.out.println("Before socket write");
            sock.out.write(socketBuffer, 0, size);
            sock.out.flush();
            System.out.println("Reading response");
            int readBytes = readPlainText(sock.in, '\n');
            System.out.println("Read bytes in response: " + readBytes);
            long iterationNumber = bytesToLong(socketBuffer);
            long numRecords = bytesToLong(socketBuffer);
            long recordsBytes = bytesToLong(socketBuffer);
            v.getValue = String.format("%d|%d|%d", iterationNumber, numRecords, recordsBytes);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        return v;
    }

    public boolean queryConfigComplete(int serverId) {
        try {
            sock_read_pivot = 0;
            Sock sock = getSock(serverId);
            socketBuffer[0] = 'R';
            socketBuffer[1] = '\n';
            sock.out.write(socketBuffer, 0, 2);
            sock.out.flush();
            int response = readPlainText(sock.in, '\n');
            int retValue = (int) bytesToLong(socketBuffer);
            socketBuffer[0] = 'a';
            if (retValue == 0) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return false;
    }

    public int stats(int serverId) {
        try {
            sock_read_pivot = 0;
            Sock sock = getSock(serverId);
            socketBuffer[0] = 's';
            int size = 1;
            socketBuffer[size] = '\n';
            size++;
            sock.out.write(socketBuffer, 0, size);
            sock.out.flush();
            readPlainText(sock.in, '!');
            return bytesToInt(socketBuffer, 0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return -1;
    }

    public void drainDB(int serverId) {
        try {
            sock_read_pivot = 0;
            Sock sock = getSock(serverId);
            socketBuffer[0] = 'S';
            int size = 1;
            socketBuffer[size] = '\n';
            size++;
            sock.out.write(socketBuffer, 0, size);
            sock.out.flush();
            readPlainText(sock.in, '!');
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

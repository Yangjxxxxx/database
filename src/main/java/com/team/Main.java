package com.team;

import com.team.db.Database;
import com.team.db.impl.DatabaseImpl;

import java.io.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Application
 */
@SuppressWarnings("all")
public class Main {

    /**
     * true: 线下;
     * false: 线上;
     */
    public static final boolean LOCAL = true;
//    public static final boolean LOCAL = false;

    public static void main(String[] args) throws IOException {
        if(LOCAL) {
            localTest();
        } else {
            onlineStart();
        }
    }

    private static
    void localTest() throws IOException {
        String databaseDir = "C:\\Users\\yangjx\\Desktop\\java\\ccf-contest\\database";
        String queryParamsFilePath = "C:\\Users\\yangjx\\Desktop\\java\\ccf-contest\\params.txt";
        String answerFilePath = "C:\\Users\\yangjx\\Desktop\\java\\ccf-contest\\out.txt";

        long startNs = System.nanoTime();
        Database db;
        if( (db = DatabaseImpl.load(databaseDir)) == null ) {
            System.out.println("Database is null.");
            System.exit(-1);
        }
        System.out.println(db);

        File answerFile = new File(answerFilePath);
        if(!answerFile.exists()) {
            answerFile.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(answerFile));
        BufferedReader reader = new BufferedReader(new FileReader(new File(queryParamsFilePath)));
        List<String> params = reader.lines().collect(Collectors.toList());
        reader.close();
        for (int i = 0; i < params.size() - 1; i++) {
            writer.write(String.valueOf(db.matchesCount(params.get(i))));
            writer.newLine();
            writer.flush();
        }
        writer.write(String.valueOf(db.matchesCount(params.get(params.size() - 1))));
        writer.flush();
        writer.close();
        System.out.println("Cost Time(ms): " + TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - startNs)));
    }

    private static void onlineStart() throws IOException {
        File workspaceDir = new File(System.getProperty("user.dir"));
        String databaseDir = workspaceDir.getParent() + File.separator + "database";
        String queryParamsFilePath = workspaceDir.getParent() + File.separator + "params.txt";
        String answerFilePath = workspaceDir.getParent() + File.separator + "out.txt";

        Database db;
        if( (db = DatabaseImpl.load(databaseDir)) == null ) {
            System.out.println("Database is null.");
            System.exit(-1);
        }

        File answerFile = new File(answerFilePath);
        if(!answerFile.exists()) {
            answerFile.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(answerFile));
        BufferedReader reader = new BufferedReader(new FileReader(new File(queryParamsFilePath)));
        List<String> params = reader.lines().collect(Collectors.toList());
        reader.close();
        StringBuilder ansBuilder = new StringBuilder(1024);
        for (int i = 0; i < params.size() - 1; i++) {
            ansBuilder.append(String.valueOf(db.matchesCount(params.get(i))))
                    .append("\n");
        }
        ansBuilder.append(String.valueOf(db.matchesCount(params.get(params.size() - 1))));
        writer.write(ansBuilder.toString());
        writer.flush();
        writer.close();
    }

}

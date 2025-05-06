package com.mongodb.quickstart;

import com.mongodb.client.*;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main {

    // Queries generated using MongoDB Compass
    private static List<Document> query(int number) {
        return switch (number) {
            case 3 -> List.of(new Document("$group",
                    new Document("_id", "$category")
                        .append("count",
                    new Document("$count",
                    new Document()))));

            case 4 -> List.of(new Document("$addFields",
                    new Document("stockNonEmpty",
                    new Document("$cond", Arrays.asList(new Document("$gt", Arrays.asList("$stock_count", 0)), 1, 0)))),
                    new Document("$group",
                    new Document("_id", "$category")
                            .append("count",
                    new Document("$sum", "$stockNonEmpty"))));

            case 5 -> List.of(new Document("$unwind",
                    new Document("path", "$actors")),
                    new Document("$group",
                    new Document("_id", "$actors")
                            .append("categories",
                    new Document("$addToSet",
                    new Document("category", "$category")))),
                    new Document("$addFields",
                    new Document("categories", "$categories.category")));

            case 6 -> List.of(new Document("$unwind",
                    new Document("path", "$actors")),
                    new Document("$group",
                    new Document("_id", "$actors")
                            .append("categories",
                    new Document("$addToSet",
                    new Document("category", "$category")))),
                    new Document("$addFields",
                    new Document("categories", "$categories.category")),
                    new Document("$match",
                    new Document("$expr",
                    new Document("$gt", Arrays.asList(new Document("$size", "$categories"), 1L)))));

            case 7 -> List.of(new Document("$unwind",
                    new Document("path", "$actors")),
                    new Document("$group",
                    new Document("_id", "$actors")
                            .append("categories",
                    new Document("$addToSet",
                    new Document("category", "$category")))),
                    new Document("$addFields",
                    new Document("categories", "$categories.category")),
                    new Document("$match",
                    new Document("categories",
                    new Document("$nin", List.of("Comedy")))));

            case 8 -> List.of(new Document("$unwind",
                    new Document("path", "$actors")),
                    new Document("$group",
                    new Document("_id", "$actors")
                            .append("categories",
                    new Document("$addToSet",
                    new Document("category", "$category")))),
                    new Document("$addFields",
                    new Document("categories", "$categories.category")),
                    new Document("$match",
                    new Document("$and", Arrays.asList(new Document("categories",
                            new Document("$in", List.of("Comedy"))),
                            new Document("categories",
                            new Document("$in", List.of("Action & Adventure")))))));

            case 9 -> List.of(new Document("$unwind",
                    new Document("path", "$actors")),
                    new Document("$group",
                    new Document("_id",
                    new Document("actors", "$actors")
                                    .append("category", "$category"))
                            .append("count",
                    new Document("$count",
                    new Document()))),
                    new Document("$match",
                    new Document("$expr",
                    new Document("$gt", Arrays.asList("$count", 1L)))));

            default -> throw new IndexOutOfBoundsException("Unexpected query: " + number);
        };
    }

    private static void prettyPrint(String string, int color, int flags) {
        final String RESET = "\033[0m";
        final String COLOR_PREFIX = "\033[38;5;";
        final String FORMAT_BOLD = "\033[1m";
        final String FORMAT_ITALIC = "\033[3m";
        final String FORMAT_UNDERLINE = "\033[4m";
        final String FORMAT_STRIKETHROUGH = "\033[9m";

        StringBuilder output = new StringBuilder(RESET);

        if (color > 0)
            output.append(COLOR_PREFIX).append(color).append("m");

        if (Flags.isBold(flags))
            output.append(FORMAT_BOLD);

        if (Flags.isItalic(flags))
            output.append(FORMAT_ITALIC);

        if (Flags.isUnderline(flags))
            output.append(FORMAT_UNDERLINE);

        if (Flags.isStrikethrough(flags))
            output.append(FORMAT_STRIKETHROUGH);

        output.append(string).append(RESET);
        System.out.print(output);
    }

    private static void prettyPrint(String string, int color) {
        prettyPrint(string, color, Flags.NONE);
    }

    public static void main(String[] args) {
        prettyPrint("Connecting to the database server...\n", Colors.GRAY);

        Instant start = Instant.now();
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase database = mongoClient.getDatabase("VideoDB");
            MongoCollection<Document> collection = database.getCollection("Videos");

            long ms = Duration.between(start, Instant.now()).toMillis();

            prettyPrint("Connected to the database server successfully!", Colors.DEFAULT);
            prettyPrint(" Took " + ms + "ms\n\n", Colors.GRAY);

            prettyPrint("Which query would you like to execute?", Colors.GREEN, Flags.UNDERLINE);
            prettyPrint(" (3-9)\n\n", Colors.GRAY);

            final String[] questions = {
                    "List the number of videos for each video category.",
                    "List the number of videos for each video category where the inventory is non-zero.",
                    "For each actor, list the video categories that actor has appeared in.",
                    "Which actors have appeared in movies in different video categories?",
                    "Which actors have not appeared in a comedy?",
                    "Which actors have appeared in both a comedy and an action adventure movie?",
                    "Which actors have appeared in the same category of movie more than once?",
            };

            for (int i = 0; i < questions.length; i++) {
                int qn = i + 3;
                prettyPrint("" + qn, Colors.GREEN, Flags.UNDERLINE | Flags.BOLD);
                prettyPrint(". " + questions[i] + "\n", Colors.DEFAULT);
            }

            System.out.println();

            Scanner scanner = new Scanner(System.in);
            int queryNum;
            do {
                String input = scanner.nextLine();

                try {
                    queryNum = Integer.parseInt(input);
                    if (queryNum >= 3 && queryNum <= 9) break;
                    prettyPrint("Error: Query must be between 3 and 9 but was " + queryNum + "\n", Colors.RED);
                } catch (NumberFormatException e) {
                    prettyPrint("Error: Query must be a number but was '" + input + "'\n", Colors.RED);
                }
            } while (true);

            System.out.println();
            prettyPrint("Executing query " + queryNum + "...\n", Colors.GRAY);
            start = Instant.now();
            AggregateIterable<Document> result = collection.aggregate(query(queryNum));
            ms = Duration.between(start, Instant.now()).toMillis();
            prettyPrint("Success!", Colors.DEFAULT);
            prettyPrint(" Took " + ms + "ms\n\n", Colors.GRAY);

            prettyPrint("Results:\n", Colors.GREEN, Flags.UNDERLINE);

            int i = 0;
            for (Document document : result) {
                System.out.println(document.toJson());
                ++i;
            }

            System.out.println();
            prettyPrint("Number of returned documents: ", Colors.DEFAULT);
            prettyPrint(i + "\n", Colors.GREEN, Flags.UNDERLINE);
        }
    }

    private static class Colors {
        public static final int DEFAULT = -1;
        public static final int BLACK = 0;
        public static final int RED = 1;
        public static final int GREEN = 2;
        public static final int WHITE = 7;
        public static final int GRAY = 244;
    }

    private static class Flags {
        public static final int NONE = 0;
        public static final int BOLD = 1;
        public static final int ITALIC = 1 << 1;
        public static final int UNDERLINE = 1 << 2;
        public static final int STRIKETHROUGH = 1 << 3;

        public static boolean isBold(int flags) {
            return (flags & BOLD) != 0;
        }

        public static boolean isItalic(int flags) {
            return (flags & ITALIC) != 0;
        }

        public static boolean isUnderline(int flags) {
            return (flags & UNDERLINE) != 0;
        }

        public static boolean isStrikethrough(int flags) {
            return (flags & STRIKETHROUGH) != 0;
        }
    }
}

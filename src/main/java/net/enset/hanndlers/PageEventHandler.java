package net.enset.hanndlers;

import net.enset.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Classe Spring qui définit différents beans fonctionnels utilisés par Kafka Streams.
 * Elle contient :
 *  - un consommateur pour afficher les événements reçus
 *  - un fournisseur pour générer des événements aléatoires
 *  - une fonction de traitement (stream) pour agréger les données en temps réel
 */
@Component
public class PageEventHandler {

    /**
     * Bean Consumer : Consomme des objets PageEvent reçus depuis un topic Kafka.
     * À chaque événement reçu, il affiche le contenu dans la console.
     *
     * @return un consommateur de PageEvent
     */
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (input) -> {
            System.out.println("************");
            System.out.println(input.toString()); // Affiche l'événement reçu
            System.out.println("************");
        };
    }

    /**
     * Bean Supplier : Génère des événements PageEvent aléatoires à intervalles réguliers.
     * Ce bean est utile pour tester le flux Kafka sans producteur externe.
     *
     * @return un fournisseur de PageEvent
     */
    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> {
            // Création d’un nouvel événement avec des valeurs aléatoires
            return new PageEvent(
                    Math.random() > 0.5 ? "P1" : "P2", // Nom de la page (P1 ou P2)
                    Math.random() > 0.5 ? "U1" : "U2", // Utilisateur (U1 ou U2)
                    new Date(),                         // Date actuelle
                    10 + new Random().nextInt(10000)     // Durée entre 10 et 10 010 ms
            );
        };
    }

    /**
     * Bean Function : définit une topologie Kafka Streams.
     * Elle prend en entrée un flux (KStream) de paires (clé, PageEvent)
     * et renvoie un nouveau flux (KStream) contenant le nombre d’événements par page
     * sur une fenêtre de 5 secondes.
     *
     * @return une fonction transformant un flux de PageEvent en flux de statistiques (counts)
     */
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStream() {
        return (stream) ->
                stream
                        // Exemple de filtre possible : ne garder que les événements dont la durée > 100
                        //.filter((k, v) -> v.duration() > 100)

                        // Transformation du flux : la clé devient le nom de la page, la valeur est initialisée à 0
                        .map((k, v) -> new KeyValue<>(v.name(), 0L))

                        // Groupement des événements par clé (nom de la page)
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))

                        // Application d’une fenêtre de temps glissante de 5 secondes
                        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))

                        // Comptage du nombre d’événements dans chaque fenêtre
                        .count(Materialized.as("count-store"))

                        // Conversion du résultat (Windowed<String>, Long) → (String, Long)
                        .toStream()
                        .map((k, v) -> new KeyValue<>(k.key(), v));
    }
}

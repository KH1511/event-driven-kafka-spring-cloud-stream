package net.enset.controllers;

import net.enset.events.PageEvent;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Contrôleur REST Spring Boot pour interagir avec Kafka Streams.
 * Il permet d’envoyer des événements (PageEvent) à un topic Kafka
 * et de consulter des statistiques (analytics) en temps réel via un flux réactif.
 */
@RestController
public class PageEventController {

    // Objet permettant d’envoyer des messages à un topic Kafka
    private StreamBridge streamBridge;

    // Service permettant d’interroger l’état interne (state store) de Kafka Streams
    @Autowired
    private InteractiveQueryService interactiveQueryService;

    // Injection de dépendance du StreamBridge via le constructeur
    public PageEventController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    /**
     * Endpoint REST permettant d’envoyer un événement vers un topic Kafka.
     * Exemple d’appel : http://localhost:8080/publish?name=page1&topic=T1
     *
     * @param name  Nom de la page ou de l’événement
     * @param topic Nom du topic Kafka cible
     * @return L’événement envoyé
     */
    @GetMapping("/publish")
    public PageEvent send(String name, String topic) {
        // Création d’un nouvel événement PageEvent
        PageEvent event = new PageEvent(
                name,
                Math.random() > 0.5 ? "U1" : "U2",     // Génère aléatoirement un utilisateur U1 ou U2
                new Date(),                            // Date actuelle
                10 + new Random().nextInt(1000)        // Durée aléatoire entre 10 et 1010 ms
        );

        // Envoi de l’événement au topic Kafka spécifié
        streamBridge.send(topic, event);

        // Retourne l’objet pour affichage dans la réponse HTTP
        return event;
    }

    /**
     * Endpoint REST renvoyant des statistiques en temps réel sur les pages consultées.
     * Il interroge le "state store" Kafka Streams nommé "count-store" toutes les secondes
     * et renvoie les résultats sous forme de flux (Server-Sent Events).
     *
     * @return Un flux réactif contenant la fréquence des événements par page
     */
    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        // Création d’un flux réactif émettant une valeur toutes les secondes
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> {
                    // Map pour stocker les résultats : <nom_page, nombre_d’événements>
                    Map<String, Long> stringLongMap = new HashMap<>();

                    // Récupération du state store Kafka Streams nommé "count-store"
                    ReadOnlyWindowStore<String, Long> windowStore =
                            interactiveQueryService.getQueryableStore("count-store", QueryableStoreTypes.windowStore());

                    // Fenêtre de temps à interroger : 5 dernières secondes
                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000);

                    // Récupération de toutes les valeurs agrégées dans cette fenêtre
                    KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now);

                    // Parcours des résultats et stockage dans la map
                    while (fetchAll.hasNext()) {
                        KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        stringLongMap.put(next.key.key(), next.value);
                    }

                    // Retourne la map contenant les statistiques
                    return stringLongMap;
                });
    }

}

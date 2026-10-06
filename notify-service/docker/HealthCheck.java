import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;

/**
 * Container health probe for images without a shell, curl or wget (distroless).
 * Exits 0 only when the given URL answers HTTP 200.
 */
public final class HealthCheck {

    private HealthCheck() {
    }

    public static void main(String[] args) {
        try {
            var connection = (HttpURLConnection) URI.create(args[0]).toURL().openConnection();
            connection.setConnectTimeout(2_000);
            connection.setReadTimeout(3_000);
            System.exit(connection.getResponseCode() == HttpURLConnection.HTTP_OK ? 0 : 1);
        } catch (IOException | RuntimeException error) {
            System.err.println("Health check failed: " + error);
            System.exit(1);
        }
    }
}

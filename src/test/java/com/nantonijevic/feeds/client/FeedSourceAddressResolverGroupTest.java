package com.nantonijevic.feeds.client;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import com.nantonijevic.feeds.config.FeedSourceProperties;
import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
import io.netty.resolver.AbstractAddressResolver;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FeedSourceAddressResolverGroupTest {

    @Test
    void privateAddressSwitchDefaultsToOff() {
        assertThat(new FeedSourceProperties().isAllowPrivateAddresses())
                .isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("blockedRangeBoundaries")
    void blocksFirstAndLastAddressesOfEveryConfiguredRange(
            String description,
            String literalAddress
    ) throws Exception {
        assertBlocked(address(literalAddress));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("publicBoundaryNeighbors")
    void allowsPublicAddressesBesideBlockedRanges(
            String description,
            String literalAddress
    ) throws Exception {
        assertAllowed(address(literalAddress));
    }

    @Test
    void treatsIpv4MappedIpv6AsItsIpv4Address() throws Exception {
        assertBlocked(ipv6("00000000000000000000ffff7f000001"));
        assertAllowed(ipv6("00000000000000000000ffff08080808"));
    }

    @Test
    void doesNotExpandTheAnyLocalLiteralToTheWholeZeroSlashEightRange()
            throws Exception {
        assertAllowed(ipv4("0.0.0.1"));
    }

    @Test
    void rejectsMixedPublicAndPrivateAnswersRegardlessOfOrder()
            throws Exception {
        InetAddress publicAddress = ipv4("8.8.8.8");
        InetAddress privateAddress = ipv4("127.0.0.1");

        assertBlocked(List.of(publicAddress, privateAddress));
        assertBlocked(List.of(privateAddress, publicAddress));
    }

    @Test
    void allowsLoopbackWhenPrivateAddressSwitchIsEnabled() throws Exception {
        InetAddress loopbackAddress = ipv4("127.0.0.1");
        StubAddressResolverGroup delegate = new StubAddressResolverGroup(
                ignored -> List.of(loopbackAddress)
        );
        FeedSourceAddressResolverGroup resolverGroup =
                new FeedSourceAddressResolverGroup(delegate, true);

        try (delegate; resolverGroup) {
            List<InetSocketAddress> resolved = resolveAll(resolverGroup);

            assertThat(resolved)
                    .extracting(InetSocketAddress::getAddress)
                    .containsExactly(loopbackAddress);
        }
    }

    @Test
    void validatesAndReturnsOneDnsResolutionWithoutResolvingAgain()
            throws Exception {
        InetAddress publicAddress = ipv4("8.8.8.8");
        InetAddress rebindingAddress = ipv4("127.0.0.1");
        StubAddressResolverGroup delegate = new StubAddressResolverGroup(
                lookup -> lookup == 0
                        ? List.of(publicAddress)
                        : List.of(rebindingAddress)
        );
        FeedSourceAddressResolverGroup resolverGroup =
                new FeedSourceAddressResolverGroup(delegate, false);

        try (delegate; resolverGroup) {
            List<InetSocketAddress> resolved = resolveAll(resolverGroup);

            assertThat(resolved)
                    .extracting(InetSocketAddress::getAddress)
                    .containsExactly(publicAddress);
            assertThat(delegate.lookupCount()).hasValue(1);
        }
    }

    private void assertBlocked(InetAddress blockedAddress) throws Exception {
        StubAddressResolverGroup delegate = new StubAddressResolverGroup(
                ignored -> List.of(blockedAddress)
        );
        FeedSourceAddressResolverGroup resolverGroup =
                new FeedSourceAddressResolverGroup(delegate, false);

        try (delegate; resolverGroup) {
            assertThatThrownBy(() -> resolveAll(resolverGroup))
                    .isInstanceOf(InvalidFeedSourceException.class)
                    .hasMessage("feed source address is not allowed");
        }
    }

    private void assertBlocked(List<InetAddress> answers) throws Exception {
        StubAddressResolverGroup delegate = new StubAddressResolverGroup(
                ignored -> answers
        );
        FeedSourceAddressResolverGroup resolverGroup =
                new FeedSourceAddressResolverGroup(delegate, false);

        try (delegate; resolverGroup) {
            assertThatThrownBy(() -> resolveAll(resolverGroup))
                    .isInstanceOf(InvalidFeedSourceException.class)
                    .hasMessage("feed source address is not allowed");
        }
    }

    private void assertAllowed(InetAddress allowedAddress) throws Exception {
        StubAddressResolverGroup delegate = new StubAddressResolverGroup(
                ignored -> List.of(allowedAddress)
        );
        FeedSourceAddressResolverGroup resolverGroup =
                new FeedSourceAddressResolverGroup(delegate, false);

        try (delegate; resolverGroup) {
            List<InetSocketAddress> resolved = resolveAll(resolverGroup);

            assertThat(resolved)
                    .extracting(InetSocketAddress::getAddress)
                    .containsExactly(allowedAddress);
        }
    }

    private List<InetSocketAddress> resolveAll(
            FeedSourceAddressResolverGroup resolverGroup
    ) throws Exception {
        AddressResolver<InetSocketAddress> resolver =
                resolverGroup.getResolver(ImmediateEventExecutor.INSTANCE);
        Future<List<InetSocketAddress>> future = resolver.resolveAll(
                InetSocketAddress.createUnresolved("feed.test", 80)
        );
        future.sync();
        return future.getNow();
    }

    private static Stream<Arguments> blockedRangeBoundaries() {
        return Stream.of(
                Arguments.of("IPv4 loopback first", "127.0.0.0"),
                Arguments.of("IPv4 loopback last", "127.255.255.255"),
                Arguments.of("IPv4 any-local", "0.0.0.0"),
                Arguments.of("IPv4 link-local first", "169.254.0.0"),
                Arguments.of("IPv4 link-local last", "169.254.255.255"),
                Arguments.of("IPv4 site-local 10 first", "10.0.0.0"),
                Arguments.of("IPv4 site-local 10 last", "10.255.255.255"),
                Arguments.of("IPv4 site-local 172 first", "172.16.0.0"),
                Arguments.of("IPv4 site-local 172 last", "172.31.255.255"),
                Arguments.of("IPv4 site-local 192 first", "192.168.0.0"),
                Arguments.of("IPv4 site-local 192 last", "192.168.255.255"),
                Arguments.of("IPv4 CGNAT first", "100.64.0.0"),
                Arguments.of("IPv4 CGNAT last", "100.127.255.255"),
                Arguments.of("IPv4 multicast first", "224.0.0.0"),
                Arguments.of("IPv4 multicast last", "239.255.255.255"),
                Arguments.of(
                        "IPv6 any-local",
                        "00000000000000000000000000000000"
                ),
                Arguments.of(
                        "IPv6 loopback",
                        "00000000000000000000000000000001"
                ),
                Arguments.of(
                        "IPv6 link-local first",
                        "fe800000000000000000000000000000"
                ),
                Arguments.of(
                        "IPv6 link-local last",
                        "febfffffffffffffffffffffffffffff"
                ),
                Arguments.of(
                        "IPv6 ULA first",
                        "fc000000000000000000000000000000"
                ),
                Arguments.of(
                        "IPv6 ULA last",
                        "fdffffffffffffffffffffffffffffff"
                ),
                Arguments.of(
                        "IPv6 multicast first",
                        "ff000000000000000000000000000000"
                ),
                Arguments.of(
                        "IPv6 multicast last",
                        "ffffffffffffffffffffffffffffffff"
                )
        );
    }

    private static Stream<Arguments> publicBoundaryNeighbors() {
        return Stream.of(
                Arguments.of("before IPv4 loopback", "126.255.255.255"),
                Arguments.of("after IPv4 loopback", "128.0.0.0"),
                Arguments.of("before IPv4 link-local", "169.253.255.255"),
                Arguments.of("after IPv4 link-local", "169.255.0.0"),
                Arguments.of("before 10/8", "9.255.255.255"),
                Arguments.of("after 10/8", "11.0.0.0"),
                Arguments.of("before 172.16/12", "172.15.255.255"),
                Arguments.of("after 172.16/12", "172.32.0.0"),
                Arguments.of("before 192.168/16", "192.167.255.255"),
                Arguments.of("after 192.168/16", "192.169.0.0"),
                Arguments.of("before CGNAT", "100.63.255.255"),
                Arguments.of("after CGNAT", "100.128.0.0"),
                Arguments.of("before IPv4 multicast", "223.255.255.255"),
                Arguments.of("public IPv4", "8.8.8.8"),
                Arguments.of(
                        "public IPv6",
                        "20014860486000000000000000008888"
                )
        );
    }

    private static InetAddress address(String literal) {
        if (literal.contains(":") || literal.length() == 32) {
            return ipv6(literal);
        }
        return ipv4(literal);
    }

    private static InetAddress ipv4(String literal) {
        String[] parts = literal.split("\\.");
        byte[] bytes = new byte[4];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(parts[index]);
        }
        try {
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException exception) {
            throw new IllegalArgumentException(exception);
        }
    }

    private static InetAddress ipv6(String hexadecimal) {
        if (hexadecimal.length() != 32) {
            throw new IllegalArgumentException("IPv6 literal must have 32 hex digits");
        }
        byte[] bytes = new byte[16];
        for (int index = 0; index < bytes.length; index++) {
            int start = index * 2;
            bytes[index] = (byte) Integer.parseInt(
                    hexadecimal.substring(start, start + 2),
                    16
            );
        }
        try {
            return Inet6Address.getByAddress(null, bytes, -1);
        } catch (UnknownHostException exception) {
            throw new IllegalArgumentException(exception);
        }
    }

    private static final class StubAddressResolverGroup
            extends AddressResolverGroup<InetSocketAddress> {

        private final IntFunction<List<InetAddress>> answerProvider;
        private final AtomicInteger lookups = new AtomicInteger();

        private StubAddressResolverGroup(
                IntFunction<List<InetAddress>> answerProvider
        ) {
            this.answerProvider = answerProvider;
        }

        private AtomicInteger lookupCount() {
            return lookups;
        }

        @Override
        protected AddressResolver<InetSocketAddress> newResolver(
                EventExecutor executor
        ) {
            return new AbstractAddressResolver<>(
                    executor,
                    InetSocketAddress.class
            ) {
                @Override
                protected boolean doIsResolved(InetSocketAddress address) {
                    return false;
                }

                @Override
                protected void doResolve(
                        InetSocketAddress address,
                        Promise<InetSocketAddress> promise
                ) {
                    List<InetSocketAddress> answers = nextAnswers(address);
                    if (answers.isEmpty()) {
                        promise.tryFailure(new UnknownHostException(
                                address.getHostString()
                        ));
                    } else {
                        promise.trySuccess(answers.get(0));
                    }
                }

                @Override
                protected void doResolveAll(
                        InetSocketAddress address,
                        Promise<List<InetSocketAddress>> promise
                ) {
                    promise.trySuccess(nextAnswers(address));
                }

                private List<InetSocketAddress> nextAnswers(
                        InetSocketAddress address
                ) {
                    int lookup = lookups.getAndIncrement();
                    return answerProvider.apply(lookup)
                            .stream()
                            .map(ip -> new InetSocketAddress(
                                    ip,
                                    address.getPort()
                            ))
                            .toList();
                }
            };
        }
    }
}

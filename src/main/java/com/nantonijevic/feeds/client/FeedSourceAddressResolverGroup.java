package com.nantonijevic.feeds.client;

import java.net.InetSocketAddress;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
import io.netty.resolver.AbstractAddressResolver;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

public final class FeedSourceAddressResolverGroup
        extends AddressResolverGroup<InetSocketAddress> {

    private static final String BLOCKED_ADDRESS_MESSAGE =
            "feed source address is not allowed";

    private final AddressResolverGroup<InetSocketAddress> delegateGroup;
    private final boolean allowPrivateAddresses;

    public FeedSourceAddressResolverGroup(
            AddressResolverGroup<InetSocketAddress> delegateGroup,
            boolean allowPrivateAddresses
    ) {
        this.delegateGroup = delegateGroup;
        this.allowPrivateAddresses = allowPrivateAddresses;
    }

    @Override
    protected AddressResolver<InetSocketAddress> newResolver(
            EventExecutor executor
    ) {
        AddressResolver<InetSocketAddress> delegate =
                delegateGroup.getResolver(executor);

        return new ValidatingAddressResolver(
                executor,
                delegate,
                allowPrivateAddresses
        );
    }

    private static final class ValidatingAddressResolver
            extends AbstractAddressResolver<InetSocketAddress> {

        private final AddressResolver<InetSocketAddress> delegate;
        private final boolean allowPrivateAddresses;

        private ValidatingAddressResolver(
                EventExecutor executor,
                AddressResolver<InetSocketAddress> delegate,
                boolean allowPrivateAddresses
        ) {
            super(executor, InetSocketAddress.class);
            this.delegate = delegate;
            this.allowPrivateAddresses = allowPrivateAddresses;
        }

        @Override
        protected boolean doIsResolved(InetSocketAddress address) {
            return false;
        }

        @Override
        protected void doResolve(
                InetSocketAddress address,
                Promise<InetSocketAddress> promise
        ) {
            delegate.resolveAll(address).addListener(
                    (GenericFutureListener<Future<List<InetSocketAddress>>>)
                            future -> {
                                if (!future.isSuccess()) {
                                    promise.tryFailure(future.cause());
                                    return;
                                }
                                List<InetSocketAddress> resolved =
                                        future.getNow();
                                try {
                                    validateAll(resolved);
                                    if (resolved.isEmpty()) {
                                        promise.tryFailure(
                                                new UnknownHostException(
                                                        address.getHostString()
                                                )
                                        );
                                    } else {
                                        promise.trySuccess(resolved.get(0));
                                    }
                                } catch (RuntimeException exception) {
                                    promise.tryFailure(exception);
                                }
                            }
            );
        }

        @Override
        protected void doResolveAll(
                InetSocketAddress address,
                Promise<List<InetSocketAddress>> promise
        ) {
            delegate.resolveAll(address).addListener(
                    (GenericFutureListener<Future<List<InetSocketAddress>>>)
                            future -> {
                                if (!future.isSuccess()) {
                                    promise.tryFailure(future.cause());
                                    return;
                                }
                                List<InetSocketAddress> resolved =
                                        future.getNow();
                                try {
                                    validateAll(resolved);
                                    promise.trySuccess(resolved);
                                } catch (RuntimeException exception) {
                                    promise.tryFailure(exception);
                                }
                            }
            );
        }

        private void validateAll(List<InetSocketAddress> addresses) {
            if (allowPrivateAddresses) {
                return;
            }

            for (InetSocketAddress address : addresses) {
                InetAddress ipAddress = address.getAddress();
                if (ipAddress == null || isBlocked(ipAddress)) {
                    throw new InvalidFeedSourceException(
                            BLOCKED_ADDRESS_MESSAGE
                    );
                }
            }
        }

        private static boolean isBlocked(InetAddress address) {
            byte[] bytes = address.getAddress();
            if (address instanceof Inet6Address && isIpv4Mapped(bytes)) {
                return isBlockedIpv4(Arrays.copyOfRange(bytes, 12, 16));
            }
            if (address instanceof Inet4Address) {
                return isBlockedIpv4(bytes);
            }
            if (address instanceof Inet6Address) {
                return address.isAnyLocalAddress()
                        || address.isLoopbackAddress()
                        || address.isLinkLocalAddress()
                        || address.isMulticastAddress()
                        || (bytes[0] & 0xfe) == 0xfc;
            }
            return true;
        }

        private static boolean isBlockedIpv4(byte[] bytes) {
            int first = Byte.toUnsignedInt(bytes[0]);
            int second = Byte.toUnsignedInt(bytes[1]);

            return (first == 0
                    && bytes[1] == 0
                    && bytes[2] == 0
                    && bytes[3] == 0)
                    || first == 127
                    || (first == 169 && second == 254)
                    || first == 10
                    || (first == 172 && second >= 16 && second <= 31)
                    || (first == 192 && second == 168)
                    || (first == 100 && second >= 64 && second <= 127)
                    || (first >= 224 && first <= 239);
        }

        private static boolean isIpv4Mapped(byte[] bytes) {
            for (int index = 0; index < 10; index++) {
                if (bytes[index] != 0) {
                    return false;
                }
            }
            return bytes[10] == (byte) 0xff
                    && bytes[11] == (byte) 0xff;
        }
    }
}

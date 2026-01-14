# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do not open a public issue for security vulnerabilities.**

Instead, please send an email to: **your.email@example.com**

Include the following information:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

## Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial assessment**: Within 7 days
- **Fix timeline**: Depends on severity, typically within 30 days

## Security Considerations

This library handles distributed locking. Please be aware of:

1. **Clock synchronization**: Clients should use NTP. Clock skew can cause unexpected lock behavior.
2. **Network partitions**: During Redis failover, brief lock unavailability is expected.
3. **Fencing tokens**: Always use fencing tokens when writing to external systems to prevent stale operations.
4. **Redis security**: Ensure Redis is properly secured (authentication, TLS, network isolation).

## Acknowledgments

We appreciate responsible disclosure and will acknowledge security researchers who report valid vulnerabilities.
